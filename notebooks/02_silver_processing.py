# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Camada Silver: limpeza e padronização
# MAGIC Transforma o dado bruto em dado **confiável e consultável**. Cada transformação abaixo está documentada
# MAGIC (o quê, por quê e impacto), conforme a etapa 4.4 das orientações.
# MAGIC
# MAGIC | # | Transformação | Por quê | Impacto |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Datas aceitas em `AAAA-MM-DD` e `DD/MM/AAAA`, ajustadas ao último dia do mês | Formatos mistos impediriam o JOIN entre carteira, mercado e meta | Chave de data única em todas as tabelas |
# MAGIC | 2 | Conversão de texto para `DOUBLE` com `try_cast` | Valores inválidos viram `NULL` (e são tratados) em vez de derrubar o job | Colunas numéricas tipadas |
# MAGIC | 3 | `trim` + mapeamento da `classe_ativo` para a grafia canônica | "renda fixa", "RENDA FIXA" e "Renda Fixa" seriam 3 classes diferentes em um GROUP BY | 4 classes válidas |
# MAGIC | 4 | `gestor` nulo/vazio -> `Não informado` | Não perder o registro por um atributo descritivo | Registro preservado |
# MAGIC | 5 | Remoção de duplicatas exatas e depois por chave (`data_referencia`, `codigo_fundo`) | Duplicatas inflariam posição e resultado financeiro | Cada fundo/mês aparece 1 vez |
# MAGIC | 6 | Validações de negócio; reprovados vão para `silver.carteira_rejeitados` com o motivo | Dado impossível (rentabilidade de 120% ao mês, valor negativo) distorce a análise | Nada some em silêncio: tudo é rastreável |

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F

CLASSES = ["Renda Fixa", "Renda Variável", "Multimercado", "Investimentos no Exterior"]
CANONICAS = {
    "renda fixa": "Renda Fixa",
    "renda variável": "Renda Variável",
    "renda variavel": "Renda Variável",
    "multimercado": "Multimercado",
    "investimentos no exterior": "Investimentos no Exterior",
}
LIMITE_RENTABILIDADE = 50.0  # |rentabilidade mensal| acima disso é considerada impossível (erro de escala/digitação)


def data_mes(col):
    """Aceita AAAA-MM-DD e DD/MM/AAAA; devolve o último dia do mês. Inválida -> NULL (sem erro, mesmo com ANSI ligado)."""
    s = f"trim({col})"
    parsed = F.expr(f"coalesce(try_to_timestamp({s}, 'yyyy-MM-dd'), try_to_timestamp({s}, 'dd/MM/yyyy'))").cast("date")
    return F.last_day(parsed)


def num(col):
    return F.expr(f"try_cast(trim({col}) as double)")


def cadeia(regras):
    """CASE WHEN encadeado: devolve o texto da PRIMEIRA regra violada (ou NULL se o registro é válido)."""
    expr = None
    for cond, texto in regras:
        expr = F.when(cond, texto) if expr is None else expr.when(cond, texto)
    return expr


def processar(nome, tipado, chaves, regras, guardar_rejeitados):
    """Deduplica, valida e grava. Retorna (válidos, rejeitados, métricas)."""
    n_bruto = tipado.count()
    unico = tipado.dropDuplicates()
    n_dup_exatas = n_bruto - unico.count()

    com_motivo = unico.withColumn("motivo_rejeicao", cadeia(regras))
    rejeitados = com_motivo.where(F.col("motivo_rejeicao").isNotNull())
    validos_antes = com_motivo.where(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")

    n_validos_antes = validos_antes.count()
    validos = validos_antes.dropDuplicates(chaves)
    n_dup_chave = n_validos_antes - validos.count()

    metricas = {
        "tabela": nome, "linhas_bronze": n_bruto, "duplicatas_exatas_removidas": n_dup_exatas,
        "duplicatas_na_chave_removidas": n_dup_chave, "rejeitados": rejeitados.count(), "linhas_silver": validos.count(),
    }
    ts = F.current_timestamp()
    salvar(validos.drop("_source_file").withColumn("_processed_ts", ts), T(SILVER, nome))
    if guardar_rejeitados:
        salvar(rejeitados.withColumn("_processed_ts", ts), T(SILVER, f"{nome}_rejeitados"))
    return metricas


# COMMAND ----------

# MAGIC %md
# MAGIC ## Carteira

# COMMAND ----------

b = spark.table(T(BRONZE, "carteira"))

chave_classe = F.lower(F.trim(F.col("classe_ativo")))
classe = F.initcap(F.trim(F.col("classe_ativo")))
for bruto, canonica in CANONICAS.items():
    classe = F.when(chave_classe == bruto, F.lit(canonica)).otherwise(classe)

gestor_limpo = F.trim(F.col("gestor"))
carteira_tipada = b.select(
    data_mes("data_referencia").alias("data_referencia"),
    F.upper(F.trim(F.col("codigo_fundo"))).alias("codigo_fundo"),
    F.trim(F.col("nome_fundo")).alias("nome_fundo"),
    classe.alias("classe_ativo"),
    num("valor_aplicado").alias("valor_aplicado"),
    num("rentabilidade_pct").alias("rentabilidade_pct"),
    F.when(gestor_limpo.isNull() | (gestor_limpo == ""), F.lit("Não informado")).otherwise(gestor_limpo).alias("gestor"),
    F.col("_source_file"),
)

regras_carteira = [
    (F.col("data_referencia").isNull(), "data_referencia inválida ou ausente"),
    (F.col("codigo_fundo").isNull() | (F.col("codigo_fundo") == ""), "codigo_fundo ausente"),
    (F.col("classe_ativo").isNull() | ~F.col("classe_ativo").isin(*CLASSES), "classe_ativo fora do domínio"),
    (F.col("valor_aplicado").isNull() | (F.col("valor_aplicado") <= 0), "valor_aplicado ausente ou não positivo"),
    (F.col("rentabilidade_pct").isNull(), "rentabilidade_pct ausente"),
    (F.abs(F.col("rentabilidade_pct")) > LIMITE_RENTABILIDADE,
     f"rentabilidade_pct fora do intervalo plausível (+-{LIMITE_RENTABILIDADE:.0f}% a.m.)"),
]

m_carteira = processar("carteira", carteira_tipada, ["data_referencia", "codigo_fundo"], regras_carteira, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mercado (dados reais do Banco Central) e Meta atuarial

# COMMAND ----------

LIM_MERCADO = {
    "selic": (0, 50), "cdi": (0, 5), "ipca": (-2, 5), "ibovespa": (10000, 300000), "dolar": (1, 10),
    "ibovespa_var_pct": (-50, 50), "dolar_var_pct": (-50, 50), "selic_var_pp": (-5, 5),
}

bm = spark.table(T(BRONZE, "mercado"))
mercado_tipado = bm.select(
    data_mes("data_referencia").alias("data_referencia"),
    *[num(c).alias(c) for c in LIM_MERCADO],
    F.col("_source_file"),
)
regras_mercado = [(F.col("data_referencia").isNull(), "data_referencia inválida ou ausente")] + [
    (F.col(c).isNull() | (F.col(c) < lo) | (F.col(c) > hi), f"{c} ausente ou fora de [{lo}; {hi}]")
    for c, (lo, hi) in LIM_MERCADO.items()
]
m_mercado = processar("mercado", mercado_tipado, ["data_referencia"], regras_mercado, False)

bt = spark.table(T(BRONZE, "meta_atuarial"))
meta_tipada = bt.select(
    data_mes("data_referencia").alias("data_referencia"),
    num("meta_atuarial").alias("meta_atuarial"),
    F.col("_source_file"),
)
regras_meta = [
    (F.col("data_referencia").isNull(), "data_referencia inválida ou ausente"),
    (F.col("meta_atuarial").isNull() | (F.col("meta_atuarial") < 0) | (F.col("meta_atuarial") > 5),
     "meta_atuarial ausente ou fora de [0; 5]"),
]
m_meta = processar("meta_atuarial", meta_tipada, ["data_referencia"], regras_meta, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo do processamento (evidência para o relatório)

# COMMAND ----------

resumo = spark.createDataFrame(
    [(m["tabela"], m["linhas_bronze"], m["duplicatas_exatas_removidas"], m["duplicatas_na_chave_removidas"],
      m["rejeitados"], m["linhas_silver"]) for m in (m_carteira, m_mercado, m_meta)],
    "tabela string, linhas_bronze long, duplicatas_exatas_removidas long, duplicatas_na_chave_removidas long, "
    "rejeitados long, linhas_silver long",
)
display(resumo)

# COMMAND ----------

print("Registros enviados à quarentena (silver.carteira_rejeitados):")
display(spark.table(T(SILVER, "carteira_rejeitados")).orderBy("motivo_rejeicao", "codigo_fundo"))
