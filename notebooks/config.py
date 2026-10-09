# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração compartilhada do pipeline
# MAGIC Todos os notebooks executam `%run ./config` para usar os mesmos nomes de catálogo, schemas e Volume.
# MAGIC
# MAGIC **Se `CREATE CATALOG` não for permitido na sua conta Free Edition**, troque `CATALOG` abaixo pelo catálogo
# MAGIC padrão do workspace (execute `SELECT current_catalog()`), mantendo os schemas `bronze`, `silver` e `gold`.

# COMMAND ----------

CATALOG = "mvp_meta_atuarial"
BRONZE, SILVER, GOLD = "bronze", "silver", "gold"
VOLUME = "raw_files"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE}/{VOLUME}"


def T(camada: str, tabela: str) -> str:
    """Nome totalmente qualificado (catálogo.schema.tabela)."""
    return f"{CATALOG}.{camada}.{tabela}"


def salvar(df, nome: str) -> None:
    """Persiste o DataFrame como tabela gerenciada (Delta no Databricks), sobrescrevendo a anterior."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome)
    print(f"tabela gravada: {nome}")
