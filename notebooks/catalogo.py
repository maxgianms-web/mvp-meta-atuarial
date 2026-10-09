"""
catalogo.py - Catálogo de Dados (fonte única de verdade)
========================================================
Este módulo descreve TODAS as tabelas e colunas do projeto (Bronze, Silver e Gold):
descrição, tipo, domínio de valores e linhagem.

Usos:
  * notebooks/05_data_catalog.py grava estas descrições como COMMENT no Unity Catalog;
  * docs/catalogo_dados.md e a seção "Modelagem e Catálogo" do README são gerados daqui.
"""

CLASSES = ["Renda Fixa", "Renda Variável", "Multimercado", "Investimentos no Exterior"]
NAO_INF = "Não informado"

# nome_coluna -> (descrição, tipo nas camadas tipadas, domínio de valores)
COL = {
    "data_referencia": ("Data de referência da observação (último dia do mês)", "DATE",
                        "Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses)"),
    "codigo_fundo": ("Código identificador do fundo de investimento (chave natural)", "STRING",
                     "FND001 a FND012"),
    "nome_fundo": ("Nome (fictício) do fundo de investimento", "STRING", "12 nomes distintos, 1 por código"),
    "classe_ativo": ("Classe de ativo do fundo", "STRING", "; ".join(CLASSES)),
    "valor_aplicado": ("Posição do RPPS no fundo no início do mês de referência, em R$", "DOUBLE",
                       "Maior que 0"),
    "rentabilidade_pct": ("Rentabilidade do fundo no mês de referência, em % (1,25 = 1,25%)", "DOUBLE",
                          "Entre -50 e +50 % a.m. (faixa de plausibilidade)"),
    "gestor": ("Gestora (fictícia) responsável pelo fundo", "STRING",
               f"Gestora Alfa, Beta, Gama, Delta, Épsilon, Zeta; '{NAO_INF}' quando ausente (Silver)"),
    "selic": ("Meta Selic vigente no fim do mês, em % ao ano", "DOUBLE", "Entre 0 e 50 % a.a."),
    "cdi": ("CDI acumulado no mês (taxas diárias compostas), em % ao mês", "DOUBLE", "Entre 0 e 5 % a.m."),
    "ipca": ("IPCA - variação mensal, em % ao mês", "DOUBLE", "Entre -2 e 5 % a.m."),
    "ibovespa": ("Índice Ibovespa, pontos no último pregão do mês", "DOUBLE", "Entre 10.000 e 300.000 pontos"),
    "dolar": ("Dólar comercial (venda), R$ por US$, no último dia do mês", "DOUBLE", "Entre 1 e 10 R$/US$"),
    "ibovespa_var_pct": ("Variação percentual do Ibovespa em relação ao mês anterior", "DOUBLE",
                         "Entre -50 e +50 %"),
    "dolar_var_pct": ("Variação percentual do dólar em relação ao mês anterior", "DOUBLE", "Entre -50 e +50 %"),
    "selic_var_pp": ("Variação da Meta Selic em relação ao mês anterior, em pontos percentuais", "DOUBLE",
                     "Entre -5 e +5 p.p.; 0 = sem mudança"),
    "meta_atuarial": ("Meta atuarial mensal = (1+IPCA)*(1+spread)^(1/12)-1, com spread real de 5,0% a.a. (premissa), em % ao mês",
                      "DOUBLE", "Entre 0 e 5 % a.m."),
}

EXTRA = {
    "_source_file": ("Caminho do arquivo de origem no Volume (rastreabilidade)", "STRING", "Caminho em /Volumes/..."),
    "_ingestion_ts": ("Data/hora da ingestão na camada Bronze", "TIMESTAMP", "Timestamp UTC da carga"),
    "_processed_ts": ("Data/hora do processamento na camada Silver", "TIMESTAMP", "Timestamp UTC do processamento"),
    "motivo_rejeicao": ("Regra de validação que reprovou o registro (primeira regra violada)", "STRING",
                        "Ver regras em 02_silver_processing.py"),
    "resultado_financeiro": ("Resultado financeiro do fundo no mês = valor_aplicado x rentabilidade_pct / 100, em R$",
                             "DOUBLE", "Qualquer valor real (positivo ou negativo)"),
    "ano": ("Ano da data de referência", "INT", "2020 a 2025"),
    "mes": ("Mês da data de referência", "INT", "1 a 12"),
    "trimestre": ("Trimestre da data de referência", "INT", "1 a 4"),
    "ano_mes": ("Ano e mês no formato AAAA-MM", "STRING", "2020-01 a 2025-12"),
    "valor_total": ("Soma da posição de todos os fundos no mês, em R$", "DOUBLE", "Maior que 0"),
    "resultado_total": ("Soma do resultado financeiro de todos os fundos no mês, em R$", "DOUBLE", "Qualquer valor real"),
    "retorno_carteira_pct": ("Retorno mensal da carteira = resultado_total / valor_total x 100 (média ponderada pela posição), em %",
                             "DOUBLE", "Tipicamente entre -30 e +30 % a.m."),
    "excesso_sobre_meta_pp": ("Retorno da carteira menos a meta atuarial do mês, em pontos percentuais", "DOUBLE",
                              "Positivo = superou a meta"),
    "atingiu_meta": ("Indica se o retorno da carteira foi maior ou igual à meta atuarial no mês", "BOOLEAN",
                     "true / false"),
    "cenario_juros": ("Regime de juros no mês conforme selic_var_pp", "STRING",
                      "Selic em alta (>0); Selic em queda (<0); Selic estável (=0)"),
    "cenario_bolsa": ("Regime da bolsa no mês conforme ibovespa_var_pct", "STRING",
                      "Bolsa em alta (>=0); Bolsa em queda (<0)"),
    "cenario_economico": ("Combinação cenario_juros + cenario_bolsa", "STRING", "Até 6 combinações"),
    "retorno_pct": ("Retorno mensal da classe de ativo = resultado / valor x 100 (média ponderada pela posição), em %",
                    "DOUBLE", "Tipicamente entre -30 e +30 % a.m."),
    # tabela de resultados de qualidade
    "camada": ("Camada verificada", "STRING", "bronze; silver"),
    "tabela": ("Tabela verificada", "STRING", "carteira; mercado; meta_atuarial"),
    "atributo": ("Atributo (coluna) verificado, ou chave da tabela", "STRING", "Nome de coluna"),
    "dimensao": ("Dimensão de qualidade", "STRING",
                 "Completude; Consistência; Unicidade; Acurácia; Outliers"),
    "regra": ("Descrição da regra aplicada", "STRING", "Texto livre"),
    "qtd_verificada": ("Quantidade de registros avaliados pela regra", "BIGINT", ">= 0"),
    "qtd_problemas": ("Quantidade de registros que violam a regra", "BIGINT", ">= 0"),
    "pct_problemas": ("Percentual de registros que violam a regra", "DOUBLE", "0 a 100"),
    "executado_em": ("Data/hora da execução da verificação", "TIMESTAMP", "Timestamp"),
}

L_CART = "carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py)"
L_MERC = ("mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: "
          "432 Selic meta, 12 CDI, 433 IPCA, 1 dólar, 7 Ibovespa)")
L_META = "meta_atuarial.csv (gerado por scripts/01_baixar_dados_bcb.py: IPCA real + spread de 5,0% a.a.)"

_MERC_COLS = ["data_referencia", "selic", "cdi", "ipca", "ibovespa", "dolar",
              "ibovespa_var_pct", "dolar_var_pct", "selic_var_pp"]
_CART_COLS = ["data_referencia", "codigo_fundo", "nome_fundo", "classe_ativo",
              "valor_aplicado", "rentabilidade_pct", "gestor"]


def _t(descricao, origem, cols, extras=(), tipo=None, lin_cols=None, lin_tab=None):
    return {"descricao": descricao, "origem": origem, "cols": list(cols) + list(extras),
            "tipo": tipo, "lin_cols": lin_cols or {}, "lin_tab": lin_tab or origem}


_DEF = {
    # ------------------------------------------------------------------ BRONZE
    "bronze.carteira": _t(
        "Posição mensal (sintética) dos 12 fundos da carteira do RPPS, exatamente como recebida do CSV (tudo texto) + metadados de ingestão.",
        L_CART, _CART_COLS, ["_source_file", "_ingestion_ts"], tipo="STRING"),
    "bronze.mercado": _t(
        "Indicadores macroeconômicos mensais reais (Banco Central), como recebidos do CSV (tudo texto) + metadados de ingestão.",
        L_MERC, _MERC_COLS, ["_source_file", "_ingestion_ts"], tipo="STRING"),
    "bronze.meta_atuarial": _t(
        "Meta atuarial mensal (IPCA + 5,0% a.a.), como recebida do CSV (tudo texto) + metadados de ingestão.",
        L_META, ["data_referencia", "meta_atuarial"], ["_source_file", "_ingestion_ts"], tipo="STRING"),
    # ------------------------------------------------------------------ SILVER
    "silver.carteira": _t(
        "Posição mensal dos fundos limpa, tipada e padronizada (sem duplicatas; registros inválidos enviados a silver.carteira_rejeitados).",
        "bronze.carteira", _CART_COLS, ["_processed_ts"],
        lin_cols={"data_referencia": "bronze.carteira.data_referencia (aceita AAAA-MM-DD e DD/MM/AAAA; ajustada ao último dia do mês)",
                  "classe_ativo": "bronze.carteira.classe_ativo (trim + mapeamento para a grafia canônica)",
                  "gestor": "bronze.carteira.gestor (nulo/vazio vira 'Não informado')"}),
    "silver.carteira_rejeitados": _t(
        "Quarentena: registros da carteira reprovados nas validações da Silver, com o motivo da rejeição.",
        "bronze.carteira", _CART_COLS, ["motivo_rejeicao", "_source_file", "_processed_ts"],
        lin_cols={"motivo_rejeicao": "Calculado em 02_silver_processing.py"}),
    "silver.mercado": _t(
        "Indicadores macroeconômicos mensais limpos e tipados (1 linha por mês).",
        "bronze.mercado", _MERC_COLS, ["_processed_ts"]),
    "silver.meta_atuarial": _t(
        "Meta atuarial mensal limpa e tipada (1 linha por mês).",
        "bronze.meta_atuarial", ["data_referencia", "meta_atuarial"], ["_processed_ts"]),
    "silver.dq_checks": _t(
        "Resultados das verificações de qualidade de dados (por camada, tabela, atributo e dimensão).",
        "04_data_quality.py", ["camada", "tabela", "atributo", "dimensao", "regra", "qtd_verificada",
                               "qtd_problemas", "pct_problemas", "executado_em"],
        lin_cols={c: "Calculado em 04_data_quality.py" for c in
                  ["camada", "tabela", "atributo", "dimensao", "regra", "qtd_verificada",
                   "qtd_problemas", "pct_problemas", "executado_em"]}),
    # ------------------------------------------------------------------ GOLD
    "gold.dim_tempo": _t(
        "Dimensão Tempo: um registro por mês analisado, com atributos de calendário. Chave: data_referencia.",
        "silver.mercado", ["data_referencia", "ano", "mes", "trimestre", "ano_mes"],
        lin_cols={c: "Derivada de silver.mercado.data_referencia" for c in ["ano", "mes", "trimestre", "ano_mes"]}),
    "gold.dim_fundo": _t(
        "Dimensão Fundo: um registro por fundo de investimento. Chave: codigo_fundo.",
        "silver.carteira", ["codigo_fundo", "nome_fundo", "classe_ativo", "gestor"],
        lin_cols={"gestor": "silver.carteira.gestor (ignora 'Não informado' quando o fundo tem gestor em outro mês)"}),
    "gold.fato_performance": _t(
        "Fato Performance: granularidade fundo x mês. Posição, rentabilidade e resultado financeiro. FKs: data_referencia (dim_tempo), codigo_fundo (dim_fundo).",
        "silver.carteira", ["data_referencia", "codigo_fundo", "valor_aplicado", "rentabilidade_pct",
                            "resultado_financeiro"],
        lin_cols={"resultado_financeiro": "Calculado: silver.carteira.valor_aplicado x silver.carteira.rentabilidade_pct / 100"}),
    "gold.fato_mercado": _t(
        "Fato Mercado: granularidade mês. Indicadores macroeconômicos e meta atuarial. FK: data_referencia (dim_tempo).",
        "silver.mercado + silver.meta_atuarial", _MERC_COLS + ["meta_atuarial"],
        lin_cols={**{c: f"silver.mercado.{c}" for c in _MERC_COLS},
                  "meta_atuarial": "silver.meta_atuarial.meta_atuarial (JOIN por data_referencia)"}),
    "gold.analitico_carteira_mensal": _t(
        "Tabela analítica: 1 linha por mês com retorno da carteira, meta atuarial, indicadores e cenário econômico (responde P1 e P5).",
        "gold.fato_performance + gold.fato_mercado + gold.dim_tempo",
        ["data_referencia", "ano", "trimestre", "valor_total", "resultado_total", "retorno_carteira_pct",
         "meta_atuarial", "excesso_sobre_meta_pp", "atingiu_meta", "selic", "cdi", "ipca", "ibovespa",
         "ibovespa_var_pct", "selic_var_pp", "cenario_juros", "cenario_bolsa", "cenario_economico"],
        lin_cols={"data_referencia": "gold.fato_performance.data_referencia",
                  "ano": "gold.dim_tempo.ano", "trimestre": "gold.dim_tempo.trimestre",
                  "valor_total": "SUM(gold.fato_performance.valor_aplicado) por mês",
                  "resultado_total": "SUM(gold.fato_performance.resultado_financeiro) por mês",
                  "retorno_carteira_pct": "Calculado: resultado_total / valor_total x 100",
                  "meta_atuarial": "gold.fato_mercado.meta_atuarial", "selic": "gold.fato_mercado.selic",
                  "cdi": "gold.fato_mercado.cdi", "ipca": "gold.fato_mercado.ipca",
                  "ibovespa": "gold.fato_mercado.ibovespa",
                  "ibovespa_var_pct": "gold.fato_mercado.ibovespa_var_pct",
                  "selic_var_pp": "gold.fato_mercado.selic_var_pp",
                  "excesso_sobre_meta_pp": "Calculado: retorno_carteira_pct - meta_atuarial",
                  "atingiu_meta": "Calculado: retorno_carteira_pct >= meta_atuarial",
                  "cenario_juros": "Regra sobre gold.fato_mercado.selic_var_pp",
                  "cenario_bolsa": "Regra sobre gold.fato_mercado.ibovespa_var_pct",
                  "cenario_economico": "Concatenação de cenario_juros e cenario_bolsa"}),
    "gold.analitico_classe_mensal": _t(
        "Tabela analítica: 1 linha por mês e classe de ativo, com posição, resultado e retorno ponderado (responde P2, P3 e P4).",
        "gold.fato_performance + gold.dim_fundo",
        ["data_referencia", "classe_ativo", "valor_aplicado", "resultado_financeiro", "retorno_pct"],
        lin_cols={"data_referencia": "gold.fato_performance.data_referencia",
                  "classe_ativo": "gold.dim_fundo.classe_ativo",
                  "valor_aplicado": "SUM(gold.fato_performance.valor_aplicado) por mês e classe",
                  "resultado_financeiro": "SUM(gold.fato_performance.resultado_financeiro) por mês e classe",
                  "retorno_pct": "Calculado: resultado_financeiro / valor_aplicado x 100"}),
}

# colunas cujo tipo difere do padrão em tabelas específicas
_TIPO_ESPECIAL = {
    ("gold.analitico_carteira_mensal", "valor_total"): "DOUBLE",
    ("silver.carteira_rejeitados", "valor_aplicado"): "DOUBLE",
}


def _montar():
    cat = {}
    for chave, d in _DEF.items():
        colunas = {}
        for c in d["cols"]:
            desc, tipo, dom = COL.get(c) or EXTRA[c]
            if d["tipo"]:  # bronze: tudo texto, exceto metadados
                if c == "_ingestion_ts":
                    pass
                elif c == "_source_file":
                    pass
                else:
                    desc, tipo = f"[Bruto, texto] {desc}", "STRING"
            tipo = _TIPO_ESPECIAL.get((chave, c), tipo)
            lin = d["lin_cols"].get(c, f"{d['origem']}.{c}" if "." in d["origem"] and "+" not in d["origem"]
                                    and not d["origem"].endswith(".py") else d["lin_tab"])
            if chave.startswith("bronze.") and c not in ("_source_file", "_ingestion_ts"):
                lin = f"Arquivo de origem: {d['origem']}, coluna {c}"
            if c in ("_source_file", "_ingestion_ts"):
                lin = "Gerado na ingestão (01_bronze_ingestion.py)"
            if c == "_processed_ts":
                lin = "Gerado no processamento (02_silver_processing.py)"
            colunas[c] = {"descricao": desc, "tipo": tipo, "dominio": dom, "linhagem": lin}
        cat[chave] = {"descricao": d["descricao"], "linhagem": d["lin_tab"], "colunas": colunas}
    return cat


CATALOGO = _montar()


def comentario_coluna(info):
    return f"{info['descricao']} | Tipo: {info['tipo']} | Domínio: {info['dominio']} | Linhagem: {info['linhagem']}"


def comentario_tabela(meta):
    return f"{meta['descricao']} | Linhagem: {meta['linhagem']}"


def gerar_markdown(camadas=("bronze", "silver", "gold"), nivel=4, abrir=("gold",)):
    """Gera o catálogo em Markdown (uma tabela por tabela do modelo)."""
    h = "#" * nivel
    out = []
    nomes = {"bronze": "Bronze (dado bruto)", "silver": "Silver (dado limpo e padronizado)",
             "gold": "Gold (modelo analítico)"}
    for camada in camadas:
        out.append(f"{'#' * (nivel - 1)} Camada {nomes[camada]}\n")
        tabelas = [k for k in CATALOGO if k.startswith(camada + ".")]
        corpo = []
        for chave in tabelas:
            meta = CATALOGO[chave]
            corpo.append(f"{h} `{chave}`\n")
            corpo.append(f"**Descrição:** {meta['descricao']}\n")
            corpo.append(f"**Linhagem da tabela:** {meta['linhagem']}\n")
            corpo.append("| Campo | Descrição | Tipo | Domínio de valores | Linhagem |")
            corpo.append("|---|---|---|---|---|")
            for c, i in meta["colunas"].items():
                esc = lambda s: s.replace("|", "\\|")  # noqa: E731
                corpo.append(f"| `{c}` | {esc(i['descricao'])} | {i['tipo']} | {esc(i['dominio'])} | {esc(i['linhagem'])} |")
            corpo.append("")
        if camada in abrir:
            out.extend(corpo)
        else:
            out.append("<details><summary>Expandir catálogo desta camada</summary>\n")
            out.extend(corpo)
            out.append("</details>\n")
    return "\n".join(out)


if __name__ == "__main__":
    print(gerar_markdown())
