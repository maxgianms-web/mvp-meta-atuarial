# Catálogo de Dados

Gerado automaticamente a partir de `notebooks/catalogo.py` (fonte única; os mesmos textos são gravados como COMMENT no Unity Catalog pelo notebook `05_data_catalog.py`).

## Camada Bronze (dado bruto)

### `bronze.carteira`

**Descrição:** Posição mensal (sintética) dos 12 fundos da carteira do RPPS, exatamente como recebida do CSV (tudo texto) + metadados de ingestão.

**Linhagem da tabela:** carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py)

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | [Bruto, texto] Data de referência da observação (último dia do mês) | STRING | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna data_referencia |
| `codigo_fundo` | [Bruto, texto] Código identificador do fundo de investimento (chave natural) | STRING | FND001 a FND012 | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna codigo_fundo |
| `nome_fundo` | [Bruto, texto] Nome (fictício) do fundo de investimento | STRING | 12 nomes distintos, 1 por código | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna nome_fundo |
| `classe_ativo` | [Bruto, texto] Classe de ativo do fundo | STRING | Renda Fixa; Renda Variável; Multimercado; Investimentos no Exterior | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna classe_ativo |
| `valor_aplicado` | [Bruto, texto] Posição do RPPS no fundo no início do mês de referência, em R$ | STRING | Maior que 0 | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna valor_aplicado |
| `rentabilidade_pct` | [Bruto, texto] Rentabilidade do fundo no mês de referência, em % (1,25 = 1,25%) | STRING | Entre -50 e +50 % a.m. (faixa de plausibilidade) | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna rentabilidade_pct |
| `gestor` | [Bruto, texto] Gestora (fictícia) responsável pelo fundo | STRING | Gestora Alfa, Beta, Gama, Delta, Épsilon, Zeta; 'Não informado' quando ausente (Silver) | Arquivo de origem: carteira.csv (gerado por scripts/02_gerar_carteira_sintetica.py), coluna gestor |
| `_source_file` | Caminho do arquivo de origem no Volume (rastreabilidade) | STRING | Caminho em /Volumes/... | Gerado na ingestão (01_bronze_ingestion.py) |
| `_ingestion_ts` | Data/hora da ingestão na camada Bronze | TIMESTAMP | Timestamp UTC da carga | Gerado na ingestão (01_bronze_ingestion.py) |

### `bronze.mercado`

**Descrição:** Indicadores macroeconômicos mensais reais (Banco Central), como recebidos do CSV (tudo texto) + metadados de ingestão.

**Linhagem da tabela:** mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance)

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | [Bruto, texto] Data de referência da observação (último dia do mês) | STRING | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna data_referencia |
| `selic` | [Bruto, texto] Meta Selic vigente no fim do mês, em % ao ano | STRING | Entre 0 e 50 % a.a. | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna selic |
| `cdi` | [Bruto, texto] CDI acumulado no mês (taxas diárias compostas), em % ao mês | STRING | Entre 0 e 5 % a.m. | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna cdi |
| `ipca` | [Bruto, texto] IPCA - variação mensal, em % ao mês | STRING | Entre -2 e 5 % a.m. | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna ipca |
| `ibovespa` | [Bruto, texto] Índice Ibovespa, pontos no último pregão do mês | STRING | Entre 10.000 e 300.000 pontos | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna ibovespa |
| `dolar` | [Bruto, texto] Dólar comercial (venda), R$ por US$, no último dia do mês | STRING | Entre 1 e 10 R$/US$ | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna dolar |
| `ibovespa_var_pct` | [Bruto, texto] Variação percentual do Ibovespa em relação ao mês anterior | STRING | Entre -50 e +50 % | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna ibovespa_var_pct |
| `dolar_var_pct` | [Bruto, texto] Variação percentual do dólar em relação ao mês anterior | STRING | Entre -50 e +50 % | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna dolar_var_pct |
| `selic_var_pp` | [Bruto, texto] Variação da Meta Selic em relação ao mês anterior, em pontos percentuais | STRING | Entre -5 e +5 p.p.; 0 = sem mudança | Arquivo de origem: mercado.csv (gerado por scripts/01_baixar_dados_bcb.py a partir das séries SGS do Banco Central: 432 Selic meta, 12 CDI, 433 IPCA, 1 dólar; e do fechamento do Ibovespa (^BVSP) obtido do Yahoo Finance), coluna selic_var_pp |
| `_source_file` | Caminho do arquivo de origem no Volume (rastreabilidade) | STRING | Caminho em /Volumes/... | Gerado na ingestão (01_bronze_ingestion.py) |
| `_ingestion_ts` | Data/hora da ingestão na camada Bronze | TIMESTAMP | Timestamp UTC da carga | Gerado na ingestão (01_bronze_ingestion.py) |

### `bronze.meta_atuarial`

**Descrição:** Meta atuarial mensal (IPCA + 5,0% a.a.), como recebida do CSV (tudo texto) + metadados de ingestão.

**Linhagem da tabela:** meta_atuarial.csv (gerado por scripts/01_baixar_dados_bcb.py: IPCA real + spread de 5,0% a.a.)

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | [Bruto, texto] Data de referência da observação (último dia do mês) | STRING | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | Arquivo de origem: meta_atuarial.csv (gerado por scripts/01_baixar_dados_bcb.py: IPCA real + spread de 5,0% a.a.), coluna data_referencia |
| `meta_atuarial` | [Bruto, texto] Meta atuarial mensal = (1+IPCA)*(1+spread)^(1/12)-1, com spread real de 5,0% a.a. (premissa), em % ao mês | STRING | Entre -2 e 5 % a.m. (pode ser levemente negativa em mês de deflação forte) | Arquivo de origem: meta_atuarial.csv (gerado por scripts/01_baixar_dados_bcb.py: IPCA real + spread de 5,0% a.a.), coluna meta_atuarial |
| `_source_file` | Caminho do arquivo de origem no Volume (rastreabilidade) | STRING | Caminho em /Volumes/... | Gerado na ingestão (01_bronze_ingestion.py) |
| `_ingestion_ts` | Data/hora da ingestão na camada Bronze | TIMESTAMP | Timestamp UTC da carga | Gerado na ingestão (01_bronze_ingestion.py) |

## Camada Silver (dado limpo e padronizado)

### `silver.carteira`

**Descrição:** Posição mensal dos fundos limpa, tipada e padronizada (sem duplicatas; registros inválidos enviados a silver.carteira_rejeitados).

**Linhagem da tabela:** bronze.carteira

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | bronze.carteira.data_referencia (aceita AAAA-MM-DD e DD/MM/AAAA; ajustada ao último dia do mês) |
| `codigo_fundo` | Código identificador do fundo de investimento (chave natural) | STRING | FND001 a FND012 | bronze.carteira.codigo_fundo |
| `nome_fundo` | Nome (fictício) do fundo de investimento | STRING | 12 nomes distintos, 1 por código | bronze.carteira.nome_fundo |
| `classe_ativo` | Classe de ativo do fundo | STRING | Renda Fixa; Renda Variável; Multimercado; Investimentos no Exterior | bronze.carteira.classe_ativo (trim + mapeamento para a grafia canônica) |
| `valor_aplicado` | Posição do RPPS no fundo no início do mês de referência, em R$ | DOUBLE | Maior que 0 | bronze.carteira.valor_aplicado |
| `rentabilidade_pct` | Rentabilidade do fundo no mês de referência, em % (1,25 = 1,25%) | DOUBLE | Entre -50 e +50 % a.m. (faixa de plausibilidade) | bronze.carteira.rentabilidade_pct |
| `gestor` | Gestora (fictícia) responsável pelo fundo | STRING | Gestora Alfa, Beta, Gama, Delta, Épsilon, Zeta; 'Não informado' quando ausente (Silver) | bronze.carteira.gestor (nulo/vazio vira 'Não informado') |
| `_processed_ts` | Data/hora do processamento na camada Silver | TIMESTAMP | Timestamp UTC do processamento | Gerado no processamento (02_silver_processing.py) |

### `silver.carteira_rejeitados`

**Descrição:** Quarentena: registros da carteira reprovados nas validações da Silver, com o motivo da rejeição.

**Linhagem da tabela:** bronze.carteira

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | bronze.carteira.data_referencia |
| `codigo_fundo` | Código identificador do fundo de investimento (chave natural) | STRING | FND001 a FND012 | bronze.carteira.codigo_fundo |
| `nome_fundo` | Nome (fictício) do fundo de investimento | STRING | 12 nomes distintos, 1 por código | bronze.carteira.nome_fundo |
| `classe_ativo` | Classe de ativo do fundo | STRING | Renda Fixa; Renda Variável; Multimercado; Investimentos no Exterior | bronze.carteira.classe_ativo |
| `valor_aplicado` | Posição do RPPS no fundo no início do mês de referência, em R$ | DOUBLE | Maior que 0 | bronze.carteira.valor_aplicado |
| `rentabilidade_pct` | Rentabilidade do fundo no mês de referência, em % (1,25 = 1,25%) | DOUBLE | Entre -50 e +50 % a.m. (faixa de plausibilidade) | bronze.carteira.rentabilidade_pct |
| `gestor` | Gestora (fictícia) responsável pelo fundo | STRING | Gestora Alfa, Beta, Gama, Delta, Épsilon, Zeta; 'Não informado' quando ausente (Silver) | bronze.carteira.gestor |
| `motivo_rejeicao` | Regra de validação que reprovou o registro (primeira regra violada) | STRING | Ver regras em 02_silver_processing.py | Calculado em 02_silver_processing.py |
| `_source_file` | Caminho do arquivo de origem no Volume (rastreabilidade) | STRING | Caminho em /Volumes/... | Gerado na ingestão (01_bronze_ingestion.py) |
| `_processed_ts` | Data/hora do processamento na camada Silver | TIMESTAMP | Timestamp UTC do processamento | Gerado no processamento (02_silver_processing.py) |

### `silver.mercado`

**Descrição:** Indicadores macroeconômicos mensais limpos e tipados (1 linha por mês).

**Linhagem da tabela:** bronze.mercado

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | bronze.mercado.data_referencia |
| `selic` | Meta Selic vigente no fim do mês, em % ao ano | DOUBLE | Entre 0 e 50 % a.a. | bronze.mercado.selic |
| `cdi` | CDI acumulado no mês (taxas diárias compostas), em % ao mês | DOUBLE | Entre 0 e 5 % a.m. | bronze.mercado.cdi |
| `ipca` | IPCA - variação mensal, em % ao mês | DOUBLE | Entre -2 e 5 % a.m. | bronze.mercado.ipca |
| `ibovespa` | Índice Ibovespa, pontos no último pregão do mês | DOUBLE | Entre 10.000 e 300.000 pontos | bronze.mercado.ibovespa |
| `dolar` | Dólar comercial (venda), R$ por US$, no último dia do mês | DOUBLE | Entre 1 e 10 R$/US$ | bronze.mercado.dolar |
| `ibovespa_var_pct` | Variação percentual do Ibovespa em relação ao mês anterior | DOUBLE | Entre -50 e +50 % | bronze.mercado.ibovespa_var_pct |
| `dolar_var_pct` | Variação percentual do dólar em relação ao mês anterior | DOUBLE | Entre -50 e +50 % | bronze.mercado.dolar_var_pct |
| `selic_var_pp` | Variação da Meta Selic em relação ao mês anterior, em pontos percentuais | DOUBLE | Entre -5 e +5 p.p.; 0 = sem mudança | bronze.mercado.selic_var_pp |
| `_processed_ts` | Data/hora do processamento na camada Silver | TIMESTAMP | Timestamp UTC do processamento | Gerado no processamento (02_silver_processing.py) |

### `silver.meta_atuarial`

**Descrição:** Meta atuarial mensal limpa e tipada (1 linha por mês).

**Linhagem da tabela:** bronze.meta_atuarial

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | bronze.meta_atuarial.data_referencia |
| `meta_atuarial` | Meta atuarial mensal = (1+IPCA)*(1+spread)^(1/12)-1, com spread real de 5,0% a.a. (premissa), em % ao mês | DOUBLE | Entre -2 e 5 % a.m. (pode ser levemente negativa em mês de deflação forte) | bronze.meta_atuarial.meta_atuarial |
| `_processed_ts` | Data/hora do processamento na camada Silver | TIMESTAMP | Timestamp UTC do processamento | Gerado no processamento (02_silver_processing.py) |

### `silver.dq_checks`

**Descrição:** Resultados das verificações de qualidade de dados (por camada, tabela, atributo e dimensão).

**Linhagem da tabela:** 04_data_quality.py

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `camada` | Camada verificada | STRING | bronze; silver | Calculado em 04_data_quality.py |
| `tabela` | Tabela verificada | STRING | carteira; mercado; meta_atuarial | Calculado em 04_data_quality.py |
| `atributo` | Atributo (coluna) verificado, ou chave da tabela | STRING | Nome de coluna | Calculado em 04_data_quality.py |
| `dimensao` | Dimensão de qualidade | STRING | Completude; Consistência; Unicidade; Acurácia; Outliers | Calculado em 04_data_quality.py |
| `regra` | Descrição da regra aplicada | STRING | Texto livre | Calculado em 04_data_quality.py |
| `qtd_verificada` | Quantidade de registros avaliados pela regra | BIGINT | >= 0 | Calculado em 04_data_quality.py |
| `qtd_problemas` | Quantidade de registros que violam a regra | BIGINT | >= 0 | Calculado em 04_data_quality.py |
| `pct_problemas` | Percentual de registros que violam a regra | DOUBLE | 0 a 100 | Calculado em 04_data_quality.py |
| `executado_em` | Data/hora da execução da verificação | TIMESTAMP | Timestamp | Calculado em 04_data_quality.py |

## Camada Gold (modelo analítico)

### `gold.dim_tempo`

**Descrição:** Dimensão Tempo: um registro por mês analisado, com atributos de calendário. Chave: data_referencia.

**Linhagem da tabela:** silver.mercado

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | silver.mercado.data_referencia |
| `ano` | Ano da data de referência | INT | 2020 a 2025 | Derivada de silver.mercado.data_referencia |
| `mes` | Mês da data de referência | INT | 1 a 12 | Derivada de silver.mercado.data_referencia |
| `trimestre` | Trimestre da data de referência | INT | 1 a 4 | Derivada de silver.mercado.data_referencia |
| `ano_mes` | Ano e mês no formato AAAA-MM | STRING | 2020-01 a 2025-12 | Derivada de silver.mercado.data_referencia |

### `gold.dim_fundo`

**Descrição:** Dimensão Fundo: um registro por fundo de investimento. Chave: codigo_fundo.

**Linhagem da tabela:** silver.carteira

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `codigo_fundo` | Código identificador do fundo de investimento (chave natural) | STRING | FND001 a FND012 | silver.carteira.codigo_fundo |
| `nome_fundo` | Nome (fictício) do fundo de investimento | STRING | 12 nomes distintos, 1 por código | silver.carteira.nome_fundo |
| `classe_ativo` | Classe de ativo do fundo | STRING | Renda Fixa; Renda Variável; Multimercado; Investimentos no Exterior | silver.carteira.classe_ativo |
| `gestor` | Gestora (fictícia) responsável pelo fundo | STRING | Gestora Alfa, Beta, Gama, Delta, Épsilon, Zeta; 'Não informado' quando ausente (Silver) | silver.carteira.gestor (ignora 'Não informado' quando o fundo tem gestor em outro mês) |

### `gold.fato_performance`

**Descrição:** Fato Performance: granularidade fundo x mês. Posição, rentabilidade e resultado financeiro. FKs: data_referencia (dim_tempo), codigo_fundo (dim_fundo).

**Linhagem da tabela:** silver.carteira

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | silver.carteira.data_referencia |
| `codigo_fundo` | Código identificador do fundo de investimento (chave natural) | STRING | FND001 a FND012 | silver.carteira.codigo_fundo |
| `valor_aplicado` | Posição do RPPS no fundo no início do mês de referência, em R$ | DOUBLE | Maior que 0 | silver.carteira.valor_aplicado |
| `rentabilidade_pct` | Rentabilidade do fundo no mês de referência, em % (1,25 = 1,25%) | DOUBLE | Entre -50 e +50 % a.m. (faixa de plausibilidade) | silver.carteira.rentabilidade_pct |
| `resultado_financeiro` | Resultado financeiro do fundo no mês = valor_aplicado x rentabilidade_pct / 100, em R$ | DOUBLE | Qualquer valor real (positivo ou negativo) | Calculado: silver.carteira.valor_aplicado x silver.carteira.rentabilidade_pct / 100 |

### `gold.fato_mercado`

**Descrição:** Fato Mercado: granularidade mês. Indicadores macroeconômicos e meta atuarial. FK: data_referencia (dim_tempo).

**Linhagem da tabela:** silver.mercado + silver.meta_atuarial

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | silver.mercado.data_referencia |
| `selic` | Meta Selic vigente no fim do mês, em % ao ano | DOUBLE | Entre 0 e 50 % a.a. | silver.mercado.selic |
| `cdi` | CDI acumulado no mês (taxas diárias compostas), em % ao mês | DOUBLE | Entre 0 e 5 % a.m. | silver.mercado.cdi |
| `ipca` | IPCA - variação mensal, em % ao mês | DOUBLE | Entre -2 e 5 % a.m. | silver.mercado.ipca |
| `ibovespa` | Índice Ibovespa, pontos no último pregão do mês | DOUBLE | Entre 10.000 e 300.000 pontos | silver.mercado.ibovespa |
| `dolar` | Dólar comercial (venda), R$ por US$, no último dia do mês | DOUBLE | Entre 1 e 10 R$/US$ | silver.mercado.dolar |
| `ibovespa_var_pct` | Variação percentual do Ibovespa em relação ao mês anterior | DOUBLE | Entre -50 e +50 % | silver.mercado.ibovespa_var_pct |
| `dolar_var_pct` | Variação percentual do dólar em relação ao mês anterior | DOUBLE | Entre -50 e +50 % | silver.mercado.dolar_var_pct |
| `selic_var_pp` | Variação da Meta Selic em relação ao mês anterior, em pontos percentuais | DOUBLE | Entre -5 e +5 p.p.; 0 = sem mudança | silver.mercado.selic_var_pp |
| `meta_atuarial` | Meta atuarial mensal = (1+IPCA)*(1+spread)^(1/12)-1, com spread real de 5,0% a.a. (premissa), em % ao mês | DOUBLE | Entre -2 e 5 % a.m. (pode ser levemente negativa em mês de deflação forte) | silver.meta_atuarial.meta_atuarial (JOIN por data_referencia) |

### `gold.analitico_carteira_mensal`

**Descrição:** Tabela analítica: 1 linha por mês com retorno da carteira, meta atuarial, indicadores e cenário econômico (responde P1 e P5).

**Linhagem da tabela:** gold.fato_performance + gold.fato_mercado + gold.dim_tempo

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | gold.fato_performance.data_referencia |
| `ano` | Ano da data de referência | INT | 2020 a 2025 | gold.dim_tempo.ano |
| `trimestre` | Trimestre da data de referência | INT | 1 a 4 | gold.dim_tempo.trimestre |
| `valor_total` | Soma da posição de todos os fundos no mês, em R$ | DOUBLE | Maior que 0 | SUM(gold.fato_performance.valor_aplicado) por mês |
| `resultado_total` | Soma do resultado financeiro de todos os fundos no mês, em R$ | DOUBLE | Qualquer valor real | SUM(gold.fato_performance.resultado_financeiro) por mês |
| `retorno_carteira_pct` | Retorno mensal da carteira = resultado_total / valor_total x 100 (média ponderada pela posição), em % | DOUBLE | Tipicamente entre -30 e +30 % a.m. | Calculado: resultado_total / valor_total x 100 |
| `meta_atuarial` | Meta atuarial mensal = (1+IPCA)*(1+spread)^(1/12)-1, com spread real de 5,0% a.a. (premissa), em % ao mês | DOUBLE | Entre -2 e 5 % a.m. (pode ser levemente negativa em mês de deflação forte) | gold.fato_mercado.meta_atuarial |
| `excesso_sobre_meta_pp` | Retorno da carteira menos a meta atuarial do mês, em pontos percentuais | DOUBLE | Positivo = superou a meta | Calculado: retorno_carteira_pct - meta_atuarial |
| `atingiu_meta` | Indica se o retorno da carteira foi maior ou igual à meta atuarial no mês | BOOLEAN | true / false | Calculado: retorno_carteira_pct >= meta_atuarial |
| `selic` | Meta Selic vigente no fim do mês, em % ao ano | DOUBLE | Entre 0 e 50 % a.a. | gold.fato_mercado.selic |
| `cdi` | CDI acumulado no mês (taxas diárias compostas), em % ao mês | DOUBLE | Entre 0 e 5 % a.m. | gold.fato_mercado.cdi |
| `ipca` | IPCA - variação mensal, em % ao mês | DOUBLE | Entre -2 e 5 % a.m. | gold.fato_mercado.ipca |
| `ibovespa` | Índice Ibovespa, pontos no último pregão do mês | DOUBLE | Entre 10.000 e 300.000 pontos | gold.fato_mercado.ibovespa |
| `ibovespa_var_pct` | Variação percentual do Ibovespa em relação ao mês anterior | DOUBLE | Entre -50 e +50 % | gold.fato_mercado.ibovespa_var_pct |
| `selic_var_pp` | Variação da Meta Selic em relação ao mês anterior, em pontos percentuais | DOUBLE | Entre -5 e +5 p.p.; 0 = sem mudança | gold.fato_mercado.selic_var_pp |
| `cenario_juros` | Regime de juros no mês conforme selic_var_pp | STRING | Selic em alta (>0); Selic em queda (<0); Selic estável (=0) | Regra sobre gold.fato_mercado.selic_var_pp |
| `cenario_bolsa` | Regime da bolsa no mês conforme ibovespa_var_pct | STRING | Bolsa em alta (>=0); Bolsa em queda (<0) | Regra sobre gold.fato_mercado.ibovespa_var_pct |
| `cenario_economico` | Combinação cenario_juros + cenario_bolsa | STRING | Até 6 combinações | Concatenação de cenario_juros e cenario_bolsa |

### `gold.analitico_classe_mensal`

**Descrição:** Tabela analítica: 1 linha por mês e classe de ativo, com posição, resultado e retorno ponderado (responde P2, P3 e P4).

**Linhagem da tabela:** gold.fato_performance + gold.dim_fundo

| Campo | Descrição | Tipo | Domínio de valores | Linhagem |
|---|---|---|---|---|
| `data_referencia` | Data de referência da observação (último dia do mês) | DATE | Último dia de cada mês entre 2020-01-31 e 2025-12-31 (72 meses) | gold.fato_performance.data_referencia |
| `classe_ativo` | Classe de ativo do fundo | STRING | Renda Fixa; Renda Variável; Multimercado; Investimentos no Exterior | gold.dim_fundo.classe_ativo |
| `valor_aplicado` | Posição do RPPS no fundo no início do mês de referência, em R$ | DOUBLE | Maior que 0 | SUM(gold.fato_performance.valor_aplicado) por mês e classe |
| `resultado_financeiro` | Resultado financeiro do fundo no mês = valor_aplicado x rentabilidade_pct / 100, em R$ | DOUBLE | Qualquer valor real (positivo ou negativo) | SUM(gold.fato_performance.resultado_financeiro) por mês e classe |
| `retorno_pct` | Retorno mensal da classe de ativo = resultado / valor x 100 (média ponderada pela posição), em % | DOUBLE | Tipicamente entre -30 e +30 % a.m. | Calculado: resultado_financeiro / valor_aplicado x 100 |
