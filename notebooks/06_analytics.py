# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Análise: respondendo as perguntas de negócio
# MAGIC
# MAGIC | # | Pergunta |
# MAGIC |---|---|
# MAGIC | **P1** | A carteira atingiu a meta atuarial? Em quantos meses e no acumulado do período? |
# MAGIC | **P2** | Qual classe de ativo mais contribuiu para o resultado financeiro? |
# MAGIC | **P3** | Existe relação entre a Selic e o retorno dos fundos de **renda fixa**? |
# MAGIC | **P4** | Existe relação entre o Ibovespa e o retorno dos fundos de **renda variável**? |
# MAGIC | **P5** | Em quais cenários econômicos a carteira supera a meta com mais frequência? |
# MAGIC
# MAGIC > **Leitura obrigatória dos resultados de P3 e P4:** a carteira é **sintética** e a rentabilidade de cada fundo foi
# MAGIC > construída como função do mercado real (ver `scripts/02_gerar_carteira_sintetica.py`). As correlações
# MAGIC > demonstram a **metodologia** de análise e a coerência do pipeline; não são evidência empírica sobre fundos reais.
# MAGIC > Correlação também não implica causalidade.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

G = lambda n: T(GOLD, n)  # noqa: E731
plt.rcParams.update({"figure.figsize": (11, 4.5), "axes.grid": True, "grid.alpha": 0.3})

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 - A carteira atingiu a meta atuarial?

# COMMAND ----------

p1_resumo = spark.sql(f"""
    SELECT COUNT(*)                                                           AS meses_analisados,
           SUM(CASE WHEN atingiu_meta THEN 1 ELSE 0 END)                      AS meses_meta_atingida,
           ROUND(100.0 * AVG(CASE WHEN atingiu_meta THEN 1 ELSE 0 END), 1)    AS pct_meses_meta_atingida,
           ROUND((EXP(SUM(LN(1 + retorno_carteira_pct / 100.0))) - 1) * 100, 2) AS retorno_acumulado_pct,
           ROUND((EXP(SUM(LN(1 + meta_atuarial / 100.0))) - 1) * 100, 2)      AS meta_acumulada_pct
    FROM {G('analitico_carteira_mensal')}
""")
display(p1_resumo)

# COMMAND ----------

p1_ano = spark.sql(f"""
    SELECT ano,
           COUNT(*)                                                           AS meses,
           SUM(CASE WHEN atingiu_meta THEN 1 ELSE 0 END)                      AS meses_meta_atingida,
           ROUND((EXP(SUM(LN(1 + retorno_carteira_pct / 100.0))) - 1) * 100, 2) AS retorno_no_ano_pct,
           ROUND((EXP(SUM(LN(1 + meta_atuarial / 100.0))) - 1) * 100, 2)      AS meta_no_ano_pct
    FROM {G('analitico_carteira_mensal')}
    GROUP BY ano ORDER BY ano
""")
display(p1_ano)

# COMMAND ----------

pdf = spark.table(G("analitico_carteira_mensal")).orderBy("data_referencia").toPandas()
pdf["data_referencia"] = pd.to_datetime(pdf["data_referencia"])

fig, ax = plt.subplots()
ax.plot(pdf["data_referencia"], pdf["retorno_carteira_pct"], label="Retorno da carteira (% a.m.)", lw=1.6)
ax.plot(pdf["data_referencia"], pdf["meta_atuarial"], label="Meta atuarial (% a.m.)", lw=1.6, ls="--")
ax.set_title("P1 - Retorno mensal da carteira x meta atuarial")
ax.set_ylabel("% ao mês")
ax.legend()
plt.show()

fig, ax = plt.subplots()
cores = ["tab:green" if x >= 0 else "tab:red" for x in pdf["excesso_sobre_meta_pp"]]
ax.bar(pdf["data_referencia"], pdf["excesso_sobre_meta_pp"], width=20, color=cores)
ax.axhline(0, color="black", lw=0.8)
ax.set_title("P1 - Excesso de retorno sobre a meta (p.p.) - verde: meta atingida")
ax.set_ylabel("pontos percentuais")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 - Qual classe de ativo mais contribuiu para o resultado financeiro?

# COMMAND ----------

p2 = spark.sql(f"""
    SELECT classe_ativo,
           ROUND(SUM(resultado_financeiro), 2)                                              AS resultado_acumulado_rs,
           ROUND(100.0 * SUM(resultado_financeiro) / SUM(SUM(resultado_financeiro)) OVER (), 2) AS pct_do_resultado_total,
           ROUND(AVG(valor_aplicado), 2)                                                    AS posicao_media_rs,
           ROUND((EXP(SUM(LN(1 + retorno_pct / 100.0))) - 1) * 100, 2)                      AS retorno_acumulado_pct
    FROM {G('analitico_classe_mensal')}
    GROUP BY classe_ativo
    ORDER BY resultado_acumulado_rs DESC
""")
display(p2)

# COMMAND ----------

p2_pdf = p2.toPandas()
fig, axs = plt.subplots(1, 2, figsize=(12, 4.5))
axs[0].barh(p2_pdf["classe_ativo"], p2_pdf["resultado_acumulado_rs"] / 1e6, color="tab:blue")
axs[0].set_title("Resultado financeiro acumulado (R$ milhões)")
axs[0].invert_yaxis()
axs[1].barh(p2_pdf["classe_ativo"], p2_pdf["retorno_acumulado_pct"], color="tab:orange")
axs[1].set_title("Retorno acumulado composto (%)")
axs[1].invert_yaxis()
plt.suptitle("P2 - Contribuição por classe de ativo")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Como ler:** resultado em R$ depende do tamanho da posição; o retorno acumulado (%) mostra a eficiência da classe.
# MAGIC Uma classe pode liderar em R$ por ter mais capital alocado, sem ter o melhor retorno. Discuta as duas visões.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 - Selic x retorno da Renda Fixa

# COMMAND ----------

p3 = spark.sql(f"""
    SELECT COUNT(*)                                          AS meses,
           ROUND(CORR(rf.retorno_pct, m.selic), 4)           AS corr_retorno_vs_selic_nivel,
           ROUND(CORR(rf.retorno_pct, m.cdi), 4)             AS corr_retorno_vs_cdi_mensal,
           ROUND(CORR(rf.retorno_pct, m.selic_var_pp), 4)    AS corr_retorno_vs_variacao_selic
    FROM {G('analitico_classe_mensal')} rf
    JOIN {G('fato_mercado')} m ON m.data_referencia = rf.data_referencia
    WHERE rf.classe_ativo = 'Renda Fixa'
""")
display(p3)

# COMMAND ----------

p3_pdf = spark.sql(f"""
    SELECT rf.data_referencia, rf.retorno_pct, m.selic, m.cdi
    FROM {G('analitico_classe_mensal')} rf
    JOIN {G('fato_mercado')} m ON m.data_referencia = rf.data_referencia
    WHERE rf.classe_ativo = 'Renda Fixa'
""").toPandas()

fig, axs = plt.subplots(1, 2, figsize=(12, 4.5))
axs[0].scatter(p3_pdf["selic"], p3_pdf["retorno_pct"], alpha=0.7)
axs[0].set_xlabel("Meta Selic (% a.a.)")
axs[0].set_ylabel("Retorno da Renda Fixa (% a.m.)")
axs[0].set_title("Retorno x nível da Selic")
axs[1].scatter(p3_pdf["cdi"], p3_pdf["retorno_pct"], alpha=0.7, color="tab:orange")
axs[1].set_xlabel("CDI (% a.m.)")
axs[1].set_ylabel("Retorno da Renda Fixa (% a.m.)")
axs[1].set_title("Retorno x CDI do mês")
plt.suptitle("P3 - Renda Fixa e juros")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 - Ibovespa x retorno da Renda Variável

# COMMAND ----------

p4 = spark.sql(f"""
    SELECT COUNT(*)                                                                  AS meses,
           ROUND(CORR(rv.retorno_pct, m.ibovespa_var_pct), 4)                        AS correlacao,
           ROUND(POWER(CORR(rv.retorno_pct, m.ibovespa_var_pct), 2), 4)              AS r_quadrado,
           ROUND(COVAR_SAMP(rv.retorno_pct, m.ibovespa_var_pct) / VAR_SAMP(m.ibovespa_var_pct), 4) AS beta_vs_ibovespa
    FROM {G('analitico_classe_mensal')} rv
    JOIN {G('fato_mercado')} m ON m.data_referencia = rv.data_referencia
    WHERE rv.classe_ativo = 'Renda Variável'
""")
display(p4)

# COMMAND ----------

import numpy as np

p4_pdf = spark.sql(f"""
    SELECT rv.data_referencia, rv.retorno_pct, m.ibovespa_var_pct
    FROM {G('analitico_classe_mensal')} rv
    JOIN {G('fato_mercado')} m ON m.data_referencia = rv.data_referencia
    WHERE rv.classe_ativo = 'Renda Variável'
""").toPandas()

x, y = p4_pdf["ibovespa_var_pct"].astype(float), p4_pdf["retorno_pct"].astype(float)
coef = np.polyfit(x, y, 1)
fig, ax = plt.subplots()
ax.scatter(x, y, alpha=0.7)
xs = np.linspace(x.min(), x.max(), 50)
ax.plot(xs, coef[0] * xs + coef[1], color="tab:red", label=f"regressão: beta = {coef[0]:.2f}")
ax.set_xlabel("Variação mensal do Ibovespa (%)")
ax.set_ylabel("Retorno da Renda Variável (% a.m.)")
ax.set_title("P4 - Renda Variável x Ibovespa")
ax.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 - Em quais cenários a carteira supera a meta com mais frequência?
# MAGIC Cenário = combinação de **regime de juros** (Selic em alta / queda / estável, pela variação da meta no mês)
# MAGIC e **regime da bolsa** (Ibovespa em alta ou em queda no mês). Ver regra em `03_gold_model.py`.

# COMMAND ----------

p5 = spark.sql(f"""
    SELECT cenario_economico,
           COUNT(*)                                                      AS meses,
           ROUND(AVG(retorno_carteira_pct), 3)                           AS retorno_medio_pct,
           ROUND(AVG(excesso_sobre_meta_pp), 3)                          AS excesso_medio_pp,
           SUM(CASE WHEN atingiu_meta THEN 1 ELSE 0 END)                 AS meses_meta_atingida,
           ROUND(100.0 * AVG(CASE WHEN atingiu_meta THEN 1 ELSE 0 END), 1) AS pct_meses_meta_atingida
    FROM {G('analitico_carteira_mensal')}
    GROUP BY cenario_economico
    ORDER BY pct_meses_meta_atingida DESC, meses DESC
""")
display(p5)

# COMMAND ----------

p5_bolsa = spark.sql(f"""
    SELECT cenario_bolsa, COUNT(*) AS meses,
           ROUND(100.0 * AVG(CASE WHEN atingiu_meta THEN 1 ELSE 0 END), 1) AS pct_meses_meta_atingida
    FROM {G('analitico_carteira_mensal')} GROUP BY cenario_bolsa
""")
p5_juros = spark.sql(f"""
    SELECT cenario_juros, COUNT(*) AS meses,
           ROUND(100.0 * AVG(CASE WHEN atingiu_meta THEN 1 ELSE 0 END), 1) AS pct_meses_meta_atingida
    FROM {G('analitico_carteira_mensal')} GROUP BY cenario_juros
""")
display(p5_bolsa)
display(p5_juros)

# COMMAND ----------

p5_pdf = p5.toPandas()
fig, ax = plt.subplots(figsize=(11, 4.5))
ax.barh(p5_pdf["cenario_economico"] + "  (n=" + p5_pdf["meses"].astype(str) + ")", p5_pdf["pct_meses_meta_atingida"])
ax.set_xlabel("% dos meses em que a meta foi atingida")
ax.set_title("P5 - Frequência de meta atingida por cenário (atenção ao n de cada cenário)")
ax.invert_yaxis()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Cuidado na discussão:** cenários com poucos meses (n pequeno) não sustentam conclusões fortes.
# MAGIC Cite sempre o `n` ao comentar um percentual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Números-chave para o README (copie daqui)

# COMMAND ----------

for nome, d in {"P1 resumo": p1_resumo, "P2": p2, "P3": p3, "P4": p4}.items():
    print(f"== {nome}")
    print(d.toPandas().to_string(index=False))
