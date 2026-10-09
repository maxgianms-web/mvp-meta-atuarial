# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada Bronze: ingestão dos dados brutos
# MAGIC **Regra da Bronze:** o dado é guardado exatamente como chegou (nenhuma correção). Por isso:
# MAGIC * todas as colunas são lidas como **texto** (`inferSchema = false`) - nenhum valor é perdido ou "consertado" na leitura;
# MAGIC * são adicionados apenas **metadados de controle**: `_source_file` (arquivo de origem) e `_ingestion_ts` (data/hora da carga).
# MAGIC
# MAGIC Entrada: CSVs do Volume `raw_files`. Saída: `bronze.carteira`, `bronze.mercado`, `bronze.meta_atuarial`.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import os
from pyspark.sql import functions as F

ARQUIVOS = {  # tabela Bronze -> arquivo no Volume
    "carteira": "carteira.csv",
    "mercado": "mercado.csv",
    "meta_atuarial": "meta_atuarial.csv",
}

faltando = [a for a in ARQUIVOS.values() if not os.path.exists(f"{VOLUME_PATH}/{a}")]
if faltando:
    raise FileNotFoundError(f"Arquivos não encontrados em {VOLUME_PATH}: {faltando}. Faça o upload no Volume antes de executar.")

# COMMAND ----------

resumo = []
for tabela, arquivo in ARQUIVOS.items():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")      # tudo texto: preserva o dado original
        .option("encoding", "UTF-8")
        .load(f"{VOLUME_PATH}/{arquivo}")
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
        .withColumn("_ingestion_ts", F.current_timestamp())
    )
    salvar(df, T(BRONZE, tabela))
    resumo.append((tabela, arquivo, spark.table(T(BRONZE, tabela)).count(), len(df.columns)))

display(spark.createDataFrame(resumo, "tabela string, arquivo string, linhas long, colunas int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Amostra de cada tabela Bronze (evidência para o relatório)

# COMMAND ----------

for tabela in ARQUIVOS:
    print(f"--- {T(BRONZE, tabela)}")
    display(spark.table(T(BRONZE, tabela)).limit(5))
