# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do ambiente (Unity Catalog)
# MAGIC Cria o catálogo, os schemas das camadas **Bronze / Silver / Gold** e o **Volume** onde ficam os CSVs brutos.
# MAGIC
# MAGIC ```
# MAGIC mvp_meta_atuarial            (catálogo)
# MAGIC ├── bronze   -> tabelas brutas + Volume raw_files
# MAGIC ├── silver   -> tabelas limpas e padronizadas
# MAGIC └── gold     -> modelo dimensional e tabelas analíticas
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    raise RuntimeError(
        "Não foi possível criar o catálogo. Em contas Free Edition, use o catálogo padrão do workspace: "
        "rode `SELECT current_catalog()`, altere CATALOG no notebook `config` e execute este notebook novamente."
    ) from e

for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE}.{VOLUME}")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
print("Faça upload de carteira.csv, mercado.csv e meta_atuarial.csv para:", VOLUME_PATH)
