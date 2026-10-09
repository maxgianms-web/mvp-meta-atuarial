# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Catálogo de Dados no Unity Catalog
# MAGIC Grava, para **cada tabela e cada coluna** (Bronze, Silver e Gold), um `COMMENT` com:
# MAGIC descrição, tipo, domínio de valores e linhagem. A fonte única das descrições é `catalogo.py`
# MAGIC (a mesma usada para gerar `docs/catalogo_dados.md` e o README).
# MAGIC
# MAGIC Depois de executar, abra **Catalog Explorer** -> `mvp_meta_atuarial` -> tabela -> aba **Overview / Lineage**
# MAGIC e tire os screenshots do catálogo e da linhagem para o relatório.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from catalogo import CATALOGO, comentario_coluna, comentario_tabela


def esc(texto: str) -> str:
    return texto.replace("\\", "\\\\").replace("'", "\\'")


aplicadas, ausentes, divergencias = 0, [], []
for chave, meta in CATALOGO.items():
    camada, tabela = chave.split(".")
    nome = T(camada, tabela)
    if not spark.catalog.tableExists(nome):
        ausentes.append(nome)
        continue
    spark.sql(f"COMMENT ON TABLE {nome} IS '{esc(comentario_tabela(meta))}'")
    tipos_reais = dict(spark.table(nome).dtypes)
    for coluna, info in meta["colunas"].items():
        if coluna not in tipos_reais:
            divergencias.append((nome, coluna, "coluna do catálogo não existe na tabela", ""))
            continue
        if tipos_reais[coluna].lower() != info["tipo"].lower():
            divergencias.append((nome, coluna, f"tipo no catálogo = {info['tipo']}", f"tipo real = {tipos_reais[coluna]}"))
        spark.sql(f"ALTER TABLE {nome} ALTER COLUMN `{coluna}` COMMENT '{esc(comentario_coluna(info))}'")
        aplicadas += 1
    # colunas da tabela que o catálogo esqueceu de documentar
    for coluna in tipos_reais:
        if coluna not in meta["colunas"]:
            divergencias.append((nome, coluna, "coluna sem documentação no catálogo", ""))

print(f"Comentários de coluna aplicados: {aplicadas}")
if ausentes:
    print("Tabelas do catálogo ainda não criadas (execute os notebooks anteriores):", ausentes)
if divergencias:
    print("DIVERGÊNCIAS entre catálogo e tabelas reais (corrija em catalogo.py):")
    for d in divergencias:
        print("  ", d)
else:
    print("Catálogo 100% consistente com as tabelas existentes (nomes e tipos).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo consultável (evidência: screenshot desta tabela)

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema AS camada, table_name AS tabela, ordinal_position AS pos, column_name AS campo,
           data_type AS tipo, comment AS descricao
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ('{BRONZE}', '{SILVER}', '{GOLD}')
    ORDER BY table_schema, table_name, ordinal_position
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_schema AS camada, table_name AS tabela, comment AS descricao
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ('{BRONZE}', '{SILVER}', '{GOLD}')
    ORDER BY table_schema, table_name
"""))
