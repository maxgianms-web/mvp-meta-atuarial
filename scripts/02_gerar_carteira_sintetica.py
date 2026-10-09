#!/usr/bin/env python3
"""
02_gerar_carteira_sintetica.py
==============================
Gera dados SINTÉTICOS (fictícios) da carteira de fundos de um RPPS hipotético, a partir
dos indicadores REAIS de dados/mercado.csv (gerado por 01_baixar_dados_bcb.py).

Por que sintético? Posições reais de um regime próprio de previdência são dados sensíveis
e não podem ser enviados a ambientes externos. Os 12 fundos, gestoras e valores abaixo são
inventados; apenas o mercado (Selic, CDI, IPCA, Ibovespa, dólar) é real.

ATENÇÃO (honestidade metodológica): a rentabilidade de cada fundo é construída como função
do mercado real + ruído aleatório (ver FUNDOS). Logo, as correlações encontradas na análise
refletem as premissas deste gerador e NÃO devem ser apresentadas como descoberta empírica
sobre fundos reais. Isso precisa constar na discussão e na autoavaliação.

Anomalias deliberadas: por padrão o gerador injeta erros de qualidade bem definidos
(duplicatas, nulos, datas em outro formato, classe com grafia diferente, escala errada etc.)
para que a etapa de Qualidade de Dados tenha problemas reais a detectar e tratar.
Use --sem-anomalias para gerar uma base limpa. A semente fixa (--seed) torna tudo reprodutível.

Uso:
    python scripts/02_gerar_carteira_sintetica.py
    python scripts/02_gerar_carteira_sintetica.py --seed 7 --sem-anomalias
"""
import argparse
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

# (codigo, nome, classe, gestor, posição inicial em R$ milhões)
FUNDOS = [
    ("FND001", "Aurora Referenciado DI", "Renda Fixa", "Gestora Alfa", 320),
    ("FND002", "Boreal Crédito Privado DI", "Renda Fixa", "Gestora Beta", 210),
    ("FND003", "Cedro Tesouro Selic", "Renda Fixa", "Gestora Alfa", 280),
    ("FND004", "Delta IMA-B Títulos Públicos", "Renda Fixa", "Gestora Gama", 190),
    ("FND005", "Ébano IPCA+ Longo", "Renda Fixa", "Gestora Beta", 150),
    ("FND006", "Fênix Ações Ibovespa Ativo", "Renda Variável", "Gestora Delta", 170),
    ("FND007", "Granito Ações Dividendos", "Renda Variável", "Gestora Gama", 120),
    ("FND008", "Horizonte Small Caps", "Renda Variável", "Gestora Delta", 60),
    ("FND009", "Ipê Macro Multimercado", "Multimercado", "Gestora Épsilon", 130),
    ("FND010", "Jacarandá Long Short", "Multimercado", "Gestora Épsilon", 90),
    ("FND011", "Kappa Global BDR", "Investimentos no Exterior", "Gestora Zeta", 80),
    ("FND012", "Lótus Dólar Referenciado", "Investimentos no Exterior", "Gestora Zeta", 70),
]


def retorno_mensal(codigo: str, mk: pd.DataFrame, rng: np.random.Generator) -> np.ndarray:
    """Rentabilidade mensal (%) do fundo = função do mercado real + ruído (premissas do gerador)."""
    n = len(mk)
    cdi, ipca = mk["cdi"].to_numpy(), mk["ipca"].to_numpy()
    ibov, usd = mk["ibovespa_var_pct"].to_numpy(), mk["dolar_var_pct"].to_numpy()
    dselic = mk["selic_var_pp"].to_numpy()
    ruido = lambda mu, sd: rng.normal(mu, sd, n)  # noqa: E731

    regras = {
        # Renda fixa atrelada ao CDI (baixa volatilidade)
        "FND001": lambda: 0.98 * cdi + ruido(0, 0.02),
        "FND002": lambda: 1.03 * cdi + ruido(0, 0.05),
        "FND003": lambda: 0.99 * cdi + ruido(0, 0.01),
        # Renda fixa indexada à inflação: marcação a mercado reage a mudanças da Selic
        "FND004": lambda: 0.90 * ipca + 0.40 - 0.80 * dselic + ruido(0, 0.90),
        "FND005": lambda: 0.95 * ipca + 0.45 - 1.20 * dselic + ruido(0, 1.20),
        # Renda variável: beta em relação ao Ibovespa
        "FND006": lambda: 0.95 * ibov + 0.05 + ruido(0, 1.20),
        "FND007": lambda: 0.75 * ibov + 0.15 + ruido(0, 1.50),
        "FND008": lambda: 1.15 * ibov - 0.10 + ruido(0, 3.00),
        # Multimercado: mistura de CDI e bolsa
        "FND009": lambda: 0.60 * cdi + 0.25 * ibov + ruido(0, 0.80),
        "FND010": lambda: 0.90 * cdi + 0.10 * ibov + ruido(0, 0.60),
        # Exterior: variação do dólar + retorno externo aleatório
        "FND011": lambda: usd + ruido(0.90, 4.00),
        "FND012": lambda: usd + ruido(0.20, 0.30),
    }
    return regras[codigo]()


def gerar_base_limpa(mk: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    linhas = []
    n = len(mk)
    datas = pd.to_datetime(mk["data_referencia"]).dt.strftime("%Y-%m-%d").tolist()
    for codigo, nome, classe, gestor, inicial_mi in FUNDOS:
        r = retorno_mensal(codigo, mk, rng)
        v = np.empty(n)
        v[0] = inicial_mi * 1e6 * rng.uniform(0.9, 1.1)
        for t in range(1, n):  # posição evolui com o retorno do mês anterior + fluxo líquido aleatório
            v[t] = v[t - 1] * (1 + r[t - 1] / 100.0) * (1 + rng.normal(0.004, 0.01))
        for t in range(n):
            linhas.append(
                {
                    "data_referencia": datas[t],
                    "codigo_fundo": codigo,
                    "nome_fundo": nome,
                    "classe_ativo": classe,
                    "valor_aplicado": round(float(v[t]), 2),
                    "rentabilidade_pct": round(float(r[t]), 4),
                    "gestor": gestor,
                }
            )
    return pd.DataFrame(linhas)


def _sem_acento(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def injetar_anomalias(df: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, dict]:
    """Injeta problemas de qualidade controlados (contagens fixas, linhas escolhidas pela semente)."""
    df = df.copy()
    pool = list(rng.permutation(len(df)))

    def pegar(k):
        return [pool.pop() for _ in range(k)]

    log = {}

    ix = pegar(3)  # data em outro formato (dd/MM/yyyy)
    for i in ix:
        df.at[i, "data_referencia"] = pd.to_datetime(df.at[i, "data_referencia"]).strftime("%d/%m/%Y")
    log["data_referencia em formato dd/MM/yyyy"] = len(ix)

    ix = pegar(3)  # rentabilidade ausente
    df.loc[ix, "rentabilidade_pct"] = np.nan
    log["rentabilidade_pct nula"] = len(ix)

    ix = pegar(3)  # gestor ausente
    df.loc[ix, "gestor"] = None
    log["gestor nulo"] = len(ix)

    ix = pegar(4)  # classe_ativo com grafia inconsistente
    variantes = [str.lower, str.upper, lambda s: f"  {s} ", _sem_acento]
    for i, f in zip(ix, variantes):
        df.at[i, "classe_ativo"] = f(df.at[i, "classe_ativo"])
    log["classe_ativo com grafia inconsistente"] = len(ix)

    ix = pegar(2)  # erro de escala/digitação na rentabilidade
    for i, valor in zip(ix, (120.0, -85.0)):
        df.at[i, "rentabilidade_pct"] = valor
    log["rentabilidade_pct com erro de escala (|x|>50)"] = len(ix)

    ix = pegar(2)  # valor aplicado negativo
    for i in ix:
        df.at[i, "valor_aplicado"] = -abs(df.at[i, "valor_aplicado"])
    log["valor_aplicado negativo"] = len(ix)

    ix = pegar(6)  # linhas duplicadas (cópias exatas de linhas íntegras)
    dups = df.loc[ix]
    df = pd.concat([df, dups], ignore_index=True)
    log["linhas duplicadas"] = len(ix)
    return df, log


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mercado", default="dados/mercado.csv")
    ap.add_argument("--saida", default="dados")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sem-anomalias", action="store_true")
    args = ap.parse_args()

    mk = pd.read_csv(args.mercado)
    obrig = {"data_referencia", "cdi", "ipca", "ibovespa_var_pct", "dolar_var_pct", "selic_var_pp"}
    faltando = obrig - set(mk.columns)
    if faltando:
        raise SystemExit(f"{args.mercado} sem as colunas {sorted(faltando)}. Rode antes 01_baixar_dados_bcb.py")

    rng = np.random.default_rng(args.seed)
    base = gerar_base_limpa(mk, rng)
    log = {}
    if not args.sem_anomalias:
        base, log = injetar_anomalias(base, rng)

    saida = Path(args.saida)
    saida.mkdir(parents=True, exist_ok=True)
    base.to_csv(saida / "carteira.csv", index=False)

    print(f"carteira.csv: {len(base)} linhas ({len(FUNDOS)} fundos x {len(mk)} meses"
          f"{' + duplicatas' if log else ''}) -> {saida / 'carteira.csv'}")
    if log:
        print("Anomalias injetadas deliberadamente (documente no README):")
        for k, v in log.items():
            print(f"  - {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
