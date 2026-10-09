#!/usr/bin/env python3
"""
01_baixar_dados_bcb.py
======================
Coleta os indicadores macroeconômicos REAIS (API de dados abertos do Banco Central + Ibovespa
via Yahoo Finance) e gera dois arquivos CSV mensais prontos para upload no Databricks:

    dados/mercado.csv         -> selic, cdi, ipca, ibovespa, dolar (+ variações mensais)
    dados/meta_atuarial.csv   -> meta atuarial mensal = IPCA + spread real (premissa do trabalho)

Séries SGS utilizadas (https://api.bcb.gov.br/dados/serie/bcdata.sgs.<codigo>/dados):
    432  Meta Selic definida pelo Copom (% a.a.)           -> valor vigente no fim do mês
    12   CDI (% ao dia)                                     -> composto dia a dia dentro do mês (% a.m.)
    433  IPCA, variação mensal (%)                          -> % a.m.
    1    Dólar americano (venda), R$/US$, diária            -> último valor do mês

Ibovespa: a série 7 do SGS foi descontinuada (só tem dados até 2019), então o fechamento diário do
índice (ticker ^BVSP) é obtido do Yahoo Finance pela biblioteca `yfinance` -> último valor do mês.
Plano B: --ibovespa-manual arquivo.csv (colunas: data_referencia, ibovespa; um valor de fechamento por mês).

IMPORTANTE: rode este script no SEU computador (precisa de acesso à internet).
Ele valida os valores obtidos (faixas plausíveis) antes de gravar.

Uso:
    pip install -r requirements.txt
    python scripts/01_baixar_dados_bcb.py
    python scripts/01_baixar_dados_bcb.py --spread-aa 5.0 --saida dados
"""
import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
SERIES = {"selic": 432, "ipca": 433, "cdi_diario": 12, "dolar": 1}

# Faixas plausíveis para validação (detecta código de série errado / unidade trocada)
LIMITES = {
    "selic": (0.5, 30.0),        # % a.a.
    "cdi": (0.0, 3.0),           # % a.m.
    "ipca": (-1.5, 5.0),         # % a.m.
    "ibovespa": (20_000, 250_000),
    "dolar": (2.0, 8.0),
}


# --------------------------------------------------------------------------- coleta
def baixar_serie(codigo: int, inicio: pd.Timestamp, fim: pd.Timestamp, tentativas: int = 3) -> pd.Series:
    """Baixa uma série do SGS em janelas anuais (evita o limite de janela das séries diárias)."""
    partes = []
    for ano in range(inicio.year, fim.year + 1):
        ini = max(inicio, pd.Timestamp(ano, 1, 1))
        f = min(fim, pd.Timestamp(ano, 12, 31))
        params = {
            "formato": "json",
            "dataInicial": ini.strftime("%d/%m/%Y"),
            "dataFinal": f.strftime("%d/%m/%Y"),
        }
        dados = None
        for t in range(tentativas):
            try:
                r = requests.get(BASE_URL.format(codigo=codigo), params=params, timeout=60)
                if r.status_code == 200 and isinstance(r.json(), list):
                    dados = r.json()
                    break
            except requests.RequestException:
                pass
            time.sleep(2 * (t + 1))
        if dados is None:
            raise RuntimeError(f"Falha ao baixar a série SGS {codigo} ({params['dataInicial']}..{params['dataFinal']}).")
        if dados:
            partes.append(pd.DataFrame(dados))
        print(f"  série {codigo:>3} | {ano} | {len(dados):>4} registros")
    if not partes:
        raise RuntimeError(f"Série SGS {codigo} sem dados no período.")
    df = pd.concat(partes, ignore_index=True)
    s = pd.Series(
        pd.to_numeric(df["valor"], errors="coerce").values,
        index=pd.to_datetime(df["data"], format="%d/%m/%Y"),
        name=str(codigo),
    )
    return s[~s.index.duplicated()].sort_index().dropna()


def baixar_ibovespa_yahoo(inicio: pd.Timestamp, fim: pd.Timestamp) -> pd.Series:
    """Fechamento diário do Ibovespa (^BVSP) no Yahoo Finance, via biblioteca yfinance."""
    try:
        import yfinance as yf
    except ImportError as e:
        raise SystemExit("Falta a biblioteca yfinance. Rode: py -m pip install -r requirements.txt") from e
    h = yf.Ticker("^BVSP").history(
        start=inicio.strftime("%Y-%m-%d"),
        end=(fim + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
    )
    if h is None or h.empty or "Close" not in h:
        raise RuntimeError(
            "O Yahoo Finance não devolveu dados do Ibovespa (pode ser bloqueio temporário). "
            "Tente de novo em alguns minutos ou use --ibovespa-manual arquivo.csv"
        )
    s = h["Close"].dropna().astype(float)
    if getattr(s.index, "tz", None) is not None:
        s.index = s.index.tz_localize(None)
    s.index = s.index.normalize()
    print(f"  Ibovespa (Yahoo ^BVSP) | {len(s)} fechamentos diários")
    return s[~s.index.duplicated()].sort_index()


# --------------------------------------------------------------------------- agregação mensal
def _fim_do_mes(s: pd.Series) -> pd.Series:
    """Converte índice PeriodIndex(M) para a data do último dia do mês."""
    out = s.copy()
    out.index = out.index.to_timestamp(how="end").normalize()
    return out


def ultimo_do_mes(s: pd.Series, ultimo_periodo: pd.Period) -> pd.Series:
    """Último valor observado em cada mês; meses sem observação herdam o valor anterior (ffill)."""
    g = s.groupby(s.index.to_period("M")).last()
    idx = pd.period_range(g.index.min(), ultimo_periodo, freq="M")
    return _fim_do_mes(g.reindex(idx).ffill())


def composto_do_mes(s_pct_diaria: pd.Series) -> pd.Series:
    """Compõe taxas diárias (% a.d.) em taxa mensal (% a.m.)."""
    fator = 1.0 + s_pct_diaria / 100.0
    g = fator.groupby(s_pct_diaria.index.to_period("M")).prod()
    return _fim_do_mes((g - 1.0) * 100.0)


def meta_atuarial_mensal(ipca_pct_am: pd.Series, spread_aa_pct: float) -> pd.Series:
    """Meta = (1 + IPCA_mensal) * (1 + spread_a.a.)^(1/12) - 1, em % a.m."""
    return ((1 + ipca_pct_am / 100.0) * (1 + spread_aa_pct / 100.0) ** (1 / 12) - 1) * 100.0


# --------------------------------------------------------------------------- validação
def validar(df: pd.DataFrame) -> None:
    problemas = []
    nulos = df.isna().sum()
    if nulos.sum() > 0:
        problemas.append("existem valores nulos: " + str(nulos[nulos > 0].to_dict()))
    for col, (lo, hi) in LIMITES.items():
        if col in df and not df[col].between(lo, hi).all():
            problemas.append(
                f"'{col}' fora da faixa plausível [{lo}; {hi}] (min={df[col].min():.4f}, max={df[col].max():.4f})"
            )
    if problemas:
        msg = "\n  - ".join(problemas)
        raise ValueError(
            "Validação dos dados falhou:\n  - " + msg +
            "\nConfira as séries. Para o Ibovespa você pode usar --ibovespa-manual arquivo.csv"
        )


def ler_ibovespa_manual(caminho: str) -> pd.Series:
    df = pd.read_csv(caminho)
    df["data_referencia"] = pd.to_datetime(df["data_referencia"])
    s = pd.Series(df["ibovespa"].astype(float).values, index=df["data_referencia"]).sort_index()
    g = s.groupby(s.index.to_period("M")).last()
    return _fim_do_mes(g)


# --------------------------------------------------------------------------- principal
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--inicio", default="2020-01", help="primeiro mês do período analisado (AAAA-MM)")
    ap.add_argument("--fim", default="2025-12", help="último mês do período analisado (AAAA-MM)")
    ap.add_argument("--spread-aa", type=float, default=5.0, help="spread real da meta atuarial (%% a.a.) acima do IPCA")
    ap.add_argument("--saida", default="dados", help="pasta de saída dos CSVs")
    ap.add_argument("--ibovespa-manual", default=None, help="plano B: CSV (data_referencia, ibovespa) com 1 fechamento por mês, em vez do Yahoo")
    args = ap.parse_args()

    p_ini, p_fim = pd.Period(args.inicio, "M"), pd.Period(args.fim, "M")
    # baixamos desde o ano anterior para ter o mês anterior (variações) e o último valor vigente da Selic
    janela_ini = pd.Timestamp(p_ini.year - 1, 1, 1)
    janela_fim = p_fim.end_time.normalize()

    print("Baixando séries do SGS/Banco Central ...")
    selic_d = baixar_serie(SERIES["selic"], janela_ini, janela_fim)
    cdi_d = baixar_serie(SERIES["cdi_diario"], janela_ini, janela_fim)
    ipca_m = baixar_serie(SERIES["ipca"], janela_ini, janela_fim)
    dolar_d = baixar_serie(SERIES["dolar"], janela_ini, janela_fim)

    selic = ultimo_do_mes(selic_d, p_fim)
    cdi = composto_do_mes(cdi_d)
    ipca = ultimo_do_mes(ipca_m, p_fim)
    dolar = ultimo_do_mes(dolar_d, p_fim)

    if args.ibovespa_manual:
        print(f"Usando Ibovespa do arquivo manual: {args.ibovespa_manual}")
        ibov = ler_ibovespa_manual(args.ibovespa_manual)
    else:
        ibov = ultimo_do_mes(baixar_ibovespa_yahoo(janela_ini, janela_fim), p_fim)

    df = pd.DataFrame({"selic": selic, "cdi": cdi, "ipca": ipca, "ibovespa": ibov, "dolar": dolar})
    df["ibovespa_var_pct"] = df["ibovespa"].pct_change() * 100.0
    df["dolar_var_pct"] = df["dolar"].pct_change() * 100.0
    df["selic_var_pp"] = df["selic"].diff()

    df = df.loc[(df.index >= p_ini.end_time.normalize()) & (df.index <= p_fim.end_time.normalize())]
    df.index.name = "data_referencia"
    validar(df)

    meta = meta_atuarial_mensal(df["ipca"], args.spread_aa).rename("meta_atuarial").to_frame()

    saida = Path(args.saida)
    saida.mkdir(parents=True, exist_ok=True)
    df.round(4).reset_index().to_csv(saida / "mercado.csv", index=False, date_format="%Y-%m-%d")
    meta.round(6).reset_index().to_csv(saida / "meta_atuarial.csv", index=False, date_format="%Y-%m-%d")

    print(f"\nOK: {len(df)} meses gravados em {saida}/mercado.csv e {saida}/meta_atuarial.csv")
    print(df.round(3).head(3).to_string())
    print("...")
    print(df.round(3).tail(3).to_string())
    print(f"\nMeta atuarial = IPCA + {args.spread_aa:.2f}% a.a. (premissa do trabalho)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
