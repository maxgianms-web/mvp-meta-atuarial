# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de Dados (etapa 4.5)
# MAGIC Para **cada atributo** de cada tabela verifica as cinco dimensões pedidas nas orientações:
# MAGIC
# MAGIC | Dimensão | Pergunta |
# MAGIC |---|---|
# MAGIC | **Completude** | Existem nulos ou vazios? Em que proporção? |
# MAGIC | **Consistência** | Os valores seguem o padrão esperado (data `AAAA-MM-DD`, número válido, categoria do domínio)? |
# MAGIC | **Unicidade** | Existem duplicatas onde não deveria haver? |
# MAGIC | **Acurácia** | Os valores fazem sentido no contexto (faixas plausíveis)? |
# MAGIC | **Outliers** | Há valores extremos (critério IQR, por classe de ativo ou por fundo)? |
# MAGIC
# MAGIC **Como usar:** execute após o `01` (perfila só a Bronze = problemas detectados) e novamente após o `03`
# MAGIC (perfila também a Silver = problemas após o tratamento). O resultado fica em `silver.dq_checks`,
# MAGIC com um comparativo Bronze x Silver no final.
# MAGIC
# MAGIC > Outliers estatísticos **legítimos** (ex.: queda de bolsa em crise) são mantidos e apenas sinalizados;
# MAGIC > somente valores **impossíveis** (acurácia) são rejeitados na Silver.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

CLASSES = ["Renda Fixa", "Renda Variável", "Multimercado", "Investimentos no Exterior"]

ESPEC = {
    "carteira": dict(
        chave=["data_referencia", "codigo_fundo"],
        datas=["data_referencia"],
        numericas={"valor_aplicado": (0.01, None), "rentabilidade_pct": (-50, 50)},
        categoricas={"classe_ativo": CLASSES},
        outliers={"valor_aplicado": "codigo_fundo", "rentabilidade_pct": "classe_ativo"},
    ),
    "mercado": dict(
        chave=["data_referencia"],
        datas=["data_referencia"],
        numericas={"selic": (0, 50), "cdi": (0, 5), "ipca": (-2, 5), "ibovespa": (10000, 300000), "dolar": (1, 10),
                   "ibovespa_var_pct": (-50, 50), "dolar_var_pct": (-50, 50), "selic_var_pp": (-5, 5)},
        outliers={"ipca": None, "ibovespa_var_pct": None, "dolar_var_pct": None},
    ),
    "meta_atuarial": dict(
        chave=["data_referencia"],
        datas=["data_referencia"],
        numericas={"meta_atuarial": (-2, 5)},
    ),
}


def como_numero(c):
    return F.expr(f"try_cast(trim(cast({c} as string)) as double)")


def perfilar(camada, tabela, spec):
    nome = T(camada, tabela)
    if not spark.catalog.tableExists(nome):
        print(f"(pulando {nome}: ainda não existe)")
        return []
    df = spark.table(nome)
    colunas = [c for c in df.columns if not c.startswith("_")]
    tipos = dict(df.dtypes)
    total = df.count()
    agora = datetime.now()
    linhas = []

    def reg(atributo, dimensao, regra, verificados, problemas):
        pct = round(100.0 * problemas / verificados, 2) if verificados else 0.0
        linhas.append((camada, tabela, atributo, dimensao, regra, int(verificados), int(problemas), float(pct), agora))

    # 1) Completude - todas as colunas
    for c in colunas:
        vazios = df.where(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")).count()
        reg(c, "Completude", "Valores nulos ou vazios", total, vazios)

    # 2) Unicidade - chave e linha completa
    ch = spec["chave"]
    reg(", ".join(ch), "Unicidade", "Linhas excedentes na chave (duplicatas)", total, total - df.select(*ch).distinct().count())
    reg("(linha completa)", "Unicidade", "Linhas totalmente duplicadas", total, total - df.select(*colunas).distinct().count())

    # 3) Consistência - datas
    for c in spec.get("datas", []):
        nn = df.where(F.col(c).isNotNull())
        n = nn.count()
        if tipos[c] == "string":
            ruim = nn.where(F.expr(f"try_to_timestamp(trim({c}), 'yyyy-MM-dd') IS NULL")).count()
            reg(c, "Consistência", "Data fora do padrão AAAA-MM-DD", n, ruim)
        else:
            ruim = nn.where(F.col(c) != F.last_day(F.col(c))).count()
            reg(c, "Consistência", "Data que não é o último dia do mês", n, ruim)

    # 4) Consistência (tipo numérico) e Acurácia (faixa plausível)
    for c, (lo, hi) in spec.get("numericas", {}).items():
        v = como_numero(c)
        nn = df.where(F.col(c).isNotNull())
        n = nn.count()
        if tipos[c] == "string":
            reg(c, "Consistência", "Valor não numérico", n, nn.where(v.isNull()).count())
        fora = (v < lo if lo is not None else F.lit(False)) | (v > hi if hi is not None else F.lit(False))
        reg(c, "Acurácia", f"Fora da faixa plausível [{lo}; {hi}]", n, nn.where(v.isNotNull() & fora).count())

    # 5) Consistência (domínio categórico)
    for c, validos in spec.get("categoricas", {}).items():
        nn = df.where(F.col(c).isNotNull())
        reg(c, "Consistência", "Categoria fora do domínio (grafia exata)", nn.count(), nn.where(~F.col(c).isin(*validos)).count())

    # 6) Outliers - critério IQR (Q1 - 1,5 IQR; Q3 + 1,5 IQR), opcionalmente por grupo
    for c, grupo in spec.get("outliers", {}).items():
        g = F.lower(F.trim(F.col(grupo).cast("string"))) if grupo else F.lit("todos")
        base = df.select(como_numero(c).alias("v"), g.alias("g")).where(F.col("v").isNotNull())
        qs = base.groupBy("g").agg(F.expr("percentile_approx(v, array(0.25, 0.75), 1000)").alias("q"),
                                   F.count("*").alias("n")).collect()
        n_out, n_base = 0, 0
        for r in qs:
            if r["n"] < 8:  # grupo pequeno demais para estimar quartis
                continue
            q1, q3 = r["q"]
            iqr = q3 - q1
            lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            n_base += r["n"]
            n_out += base.where((F.col("g") == r["g"]) & ((F.col("v") < lo) | (F.col("v") > hi))).count()
        reg(c, "Outliers", f"Fora de [Q1-1,5·IQR; Q3+1,5·IQR] por {grupo or 'tabela'}", n_base, n_out)
    return linhas


# COMMAND ----------

NOME_DQ = T(SILVER, "dq_checks")
SCHEMA_DQ = ("camada string, tabela string, atributo string, dimensao string, regra string, "
             "qtd_verificada bigint, qtd_problemas bigint, pct_problemas double, executado_em timestamp")

novas, camadas_perfiladas = [], []
for camada in (BRONZE, SILVER):
    achou = False
    for tabela, spec in ESPEC.items():
        r = perfilar(camada, tabela, spec)
        novas += r
        achou = achou or bool(r)
    if achou:
        camadas_perfiladas.append(camada)

# mantém resultados de camadas que não foram perfiladas nesta execução (ex.: Bronze guardada ao rodar só a Silver)
mantidas = []
if spark.catalog.tableExists(NOME_DQ):
    mantidas = [tuple(r) for r in spark.table(NOME_DQ).where(~F.col("camada").isin(*camadas_perfiladas)).collect()]
salvar(spark.createDataFrame(mantidas + novas, SCHEMA_DQ), NOME_DQ)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problemas detectados (apenas linhas com problema, por camada)

# COMMAND ----------

display(
    spark.table(NOME_DQ)
    .where("qtd_problemas > 0")
    .orderBy("camada", "tabela", "dimensao", "atributo")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparativo Bronze x Silver (efeito do tratamento)

# COMMAND ----------

display(spark.sql(f"""
    SELECT tabela, atributo, dimensao, regra,
           MAX(CASE WHEN camada = 'bronze' THEN qtd_problemas END) AS problemas_bronze,
           MAX(CASE WHEN camada = 'silver' THEN qtd_problemas END) AS problemas_silver
    FROM {NOME_DQ}
    GROUP BY tabela, atributo, dimensao, regra
    HAVING COALESCE(MAX(CASE WHEN camada = 'bronze' THEN qtd_problemas END), 0) > 0
        OR COALESCE(MAX(CASE WHEN camada = 'silver' THEN qtd_problemas END), 0) > 0
    ORDER BY tabela, dimensao, atributo
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência de que a verificação foi feita (inclusive onde NÃO há problema)

# COMMAND ----------

display(spark.sql(f"""
    SELECT camada, tabela, dimensao, COUNT(*) AS verificacoes, SUM(qtd_problemas) AS total_problemas
    FROM {NOME_DQ}
    GROUP BY camada, tabela, dimensao
    ORDER BY camada, tabela, dimensao
"""))
