# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Camada Gold: modelo dimensional (Esquema Estrela) e tabelas analíticas
# MAGIC
# MAGIC **Dimensões:** `gold.dim_tempo` (chave `data_referencia`; une os meses de carteira e mercado) e `gold.dim_fundo` (chave `codigo_fundo`).
# MAGIC
# MAGIC **Fatos:** `gold.fato_performance` (granularidade **fundo x mês**) e `gold.fato_mercado` (granularidade **mês**).
# MAGIC
# MAGIC **Tabelas analíticas** (agregações prontas para responder as perguntas de negócio):
# MAGIC `gold.analitico_carteira_mensal` (P1 e P5) e `gold.analitico_classe_mensal` (P2, P3 e P4).
# MAGIC
# MAGIC **Decisões de modelagem**
# MAGIC * `resultado_financeiro = valor_aplicado x rentabilidade_pct / 100` é calculado na fato (métrica aditiva).
# MAGIC * O retorno da carteira/classe **não** é a média simples das rentabilidades: é `SUM(resultado) / SUM(valor)` (média ponderada pela posição).
# MAGIC * O cenário econômico é definido por regra explícita (juros: variação da Selic meta; bolsa: sinal da variação do Ibovespa).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

S = lambda n: T(SILVER, n)  # noqa: E731
G = lambda n: T(GOLD, n)    # noqa: E731

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

salvar(spark.sql(f"""
    SELECT data_referencia,
           year(data_referencia)            AS ano,
           month(data_referencia)           AS mes,
           quarter(data_referencia)         AS trimestre,
           date_format(data_referencia, 'yyyy-MM') AS ano_mes
    FROM (SELECT data_referencia FROM {S('mercado')}
          UNION
          SELECT data_referencia FROM {S('carteira')})
"""), G("dim_tempo"))

# gestor: usa o valor informado em qualquer mês; só cai em 'Não informado' se nunca foi informado
salvar(spark.sql(f"""
    SELECT codigo_fundo,
           MAX(nome_fundo)    AS nome_fundo,
           MAX(classe_ativo)  AS classe_ativo,
           COALESCE(MAX(CASE WHEN gestor <> 'Não informado' THEN gestor END), 'Não informado') AS gestor
    FROM {S('carteira')}
    GROUP BY codigo_fundo
"""), G("dim_fundo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fatos

# COMMAND ----------

salvar(spark.sql(f"""
    SELECT data_referencia, codigo_fundo, valor_aplicado, rentabilidade_pct,
           valor_aplicado * rentabilidade_pct / 100.0 AS resultado_financeiro
    FROM {S('carteira')}
"""), G("fato_performance"))

salvar(spark.sql(f"""
    SELECT m.data_referencia, m.selic, m.cdi, m.ipca, m.ibovespa, m.dolar,
           m.ibovespa_var_pct, m.dolar_var_pct, m.selic_var_pp, a.meta_atuarial
    FROM {S('mercado')} m
    JOIN {S('meta_atuarial')} a ON a.data_referencia = m.data_referencia
"""), G("fato_mercado"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas analíticas

# COMMAND ----------

salvar(spark.sql(f"""
    SELECT f.data_referencia, d.classe_ativo,
           SUM(f.valor_aplicado)        AS valor_aplicado,
           SUM(f.resultado_financeiro)  AS resultado_financeiro,
           SUM(f.resultado_financeiro) / SUM(f.valor_aplicado) * 100.0 AS retorno_pct
    FROM {G('fato_performance')} f
    JOIN {G('dim_fundo')} d ON d.codigo_fundo = f.codigo_fundo
    GROUP BY f.data_referencia, d.classe_ativo
"""), G("analitico_classe_mensal"))

salvar(spark.sql(f"""
    SELECT *, concat(cenario_juros, ' / ', cenario_bolsa) AS cenario_economico
    FROM (
        SELECT c.data_referencia, t.ano, t.trimestre,
               c.valor_total, c.resultado_total,
               c.resultado_total / c.valor_total * 100.0 AS retorno_carteira_pct,
               m.meta_atuarial,
               c.resultado_total / c.valor_total * 100.0 - m.meta_atuarial AS excesso_sobre_meta_pp,
               (c.resultado_total / c.valor_total * 100.0) >= m.meta_atuarial AS atingiu_meta,
               m.selic, m.cdi, m.ipca, m.ibovespa, m.ibovespa_var_pct, m.selic_var_pp,
               CASE WHEN m.selic_var_pp > 0 THEN 'Selic em alta'
                    WHEN m.selic_var_pp < 0 THEN 'Selic em queda'
                    ELSE 'Selic estável' END AS cenario_juros,
               CASE WHEN m.ibovespa_var_pct >= 0 THEN 'Bolsa em alta' ELSE 'Bolsa em queda' END AS cenario_bolsa
        FROM (
            SELECT data_referencia, SUM(valor_aplicado) AS valor_total, SUM(resultado_financeiro) AS resultado_total
            FROM {G('fato_performance')}
            GROUP BY data_referencia
        ) c
        JOIN {G('fato_mercado')} m ON m.data_referencia = c.data_referencia
        JOIN {G('dim_tempo')}    t ON t.data_referencia = c.data_referencia
    )
"""), G("analitico_carteira_mensal"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificações de integridade do modelo (evidência para o relatório)

# COMMAND ----------

checks = spark.sql(f"""
    SELECT 'fato_performance sem dim_fundo'  AS verificacao, COUNT(*) AS violacoes
      FROM {G('fato_performance')} f LEFT JOIN {G('dim_fundo')} d USING (codigo_fundo) WHERE d.codigo_fundo IS NULL
    UNION ALL
    SELECT 'fato_performance sem dim_tempo', COUNT(*)
      FROM {G('fato_performance')} f LEFT JOIN {G('dim_tempo')} t USING (data_referencia) WHERE t.data_referencia IS NULL
    UNION ALL
    SELECT 'fato_mercado sem dim_tempo', COUNT(*)
      FROM {G('fato_mercado')} f LEFT JOIN {G('dim_tempo')} t USING (data_referencia) WHERE t.data_referencia IS NULL
    UNION ALL
    SELECT 'duplicatas em fato_performance (fundo x mês)', COUNT(*) - COUNT(DISTINCT concat(codigo_fundo, data_referencia))
      FROM {G('fato_performance')}
    UNION ALL
    SELECT 'meses de dim_tempo sem registro em fato_performance', COUNT(*)
      FROM {G('dim_tempo')} t LEFT JOIN (SELECT DISTINCT data_referencia FROM {G('fato_performance')}) f USING (data_referencia)
      WHERE f.data_referencia IS NULL
    UNION ALL
    SELECT 'meses de dim_tempo sem registro em fato_mercado (ficam fora das análises)', COUNT(*)
      FROM {G('dim_tempo')} t LEFT JOIN {G('fato_mercado')} f USING (data_referencia) WHERE f.data_referencia IS NULL
""")
display(checks)

# COMMAND ----------

for nome in ("dim_tempo", "dim_fundo", "fato_performance", "fato_mercado", "analitico_classe_mensal", "analitico_carteira_mensal"):
    print(f"--- {G(nome)}  ({spark.table(G(nome)).count()} linhas)")
    display(spark.table(G(nome)).limit(5))
