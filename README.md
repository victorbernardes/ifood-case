# Solução Data Lake — iFood Case

![Arquitetura Data Lake NYC Taxi](arquitetura.jpg)

Pipeline de dados para corridas de táxi em NYC utilizando AWS (S3, Glue, Athena).  
O fluxo segue o modelo **Bronze → Silver → Gold**, com armazenamento em S3, processamento Spark e consultas/materializações via Athena.

---

## Índice

1. [Visão Geral](#visão-geral)  
2. [Arquitetura por Camada](#arquitetura-por-camada)  
3. [Principais Decisões](#principais-decisoes)  
4. [Esquemas de Dados](#esquemas-de-dados)  
5. [Arquivos Importantes](#arquivos-importantes-no-repositório)  
6. [Como Rodar](#como-rodar-passo-a-passo)  
7. [Cuidados Operacionais e Troubleshooting](#cuidados-operacionais-e-troubleshooting)  
8. [Permissões Necessárias](#permissoes-necessarias)  
9. [Próximos Passos / Melhorias](#proximos-passos--melhorias)  

---

## Visão Geral
A Arquitetura se baseia nas 3 camadas do Data Lake (bronze, silver e gold), onde na camada Bronze estarão os dados brutos (raw data), sem alterar o nome dos campos vindos diretamente da origem, mas adicionando timestamp do processamento, e mantendo assim a rastreabilidade. Os dados estarão num bucket S3 de Landing Zone, conforme evidência. Após isso, será processado através de um Glue, utilizando Spark conforme requisito, para a inserção na camada Bronze. Para camada Silver, outro processamento via Spark, na qual irá filtrar apenas as propriedades conforme requisito que interessam, e mantendo uma única tabela com dados normalizados, nome padronizado, e com todos os dados das corridas de ambos tipos de táxi.

Por fim, para a camada Gold, é executado diretamente por script SQL no AWS Athena, com uma seleção dos dados para visão de negócio respondendo o questionário final.

- **Bronze:** dados brutos (raw), vindos diretamente da origem sem alteração nos nomes dos campos. É adicionada apenas a coluna `ingestion_ts` para rastreabilidade. Os arquivos ficam em S3 (Landing → Bronze) particionados por `ano` e `mes`.
- **Silver:** camada curada, normalizada e padronizada. Processamento via Spark (AWS Glue) que seleciona e renomeia as colunas úteis, unificando Yellow e Green em uma única tabela.
- **Gold:** visão de negócio (agregações/mesas materializadas) executada via SQL no AWS Athena e gravada em S3 (ex.: métricas por hora / média por mês).

---

## Arquitetura por Camada

### Bronze

- Dados brutos com `ingestion_ts`  
- **Formato:** Parquet (SNAPPY)  
- **Particionamento:** `ano`, `mes`  
- **Exemplo de path:** `s3://ifood-case-bronze/taxi_yellow/`

### Silver

- Dados curados e unificados  
- **Formato:** Parquet (SNAPPY)  
- **Particionamento:** `ano`, `mes`  
- **Exemplo de path:** `s3://ifood-case-silver/corridas_taxi/`

### Gold

- Tabelas agregadas/materializações para análise  
- **Formato:** Parquet (SNAPPY) ou Athena  
- **Exemplos:**  
  - `dados_taxi_gold.media_passageiros_hora_maio`  
  - `dados_taxi_gold.media_valor_total_mes_yellow`  

---

## Principais Decisões

- **Formato de armazenamento:** Parquet (SNAPPY)  
- **Particionamento:** `ano INT`, `mes INT`  
- **Convenção de nomes:** colunas/tabelas em português  
- **Ferramenta de processamento:** Glue (Spark) para Bronze/Silver, Athena para Gold

---

## Esquemas de Dados

### Bronze — Taxi Yellow

| Coluna               | Tipo       | Observação                  |
|----------------------|-----------|-----------------------------|
| VendorID             | bigint     | Identificador do fornecedor |
| tpep_pickup_datetime | timestamp  | Início da viagem            |
| tpep_dropoff_datetime| timestamp  | Fim da viagem               |
| passenger_count      | bigint     | Número de passageiros       |
| trip_distance        | double     | Distância da viagem         |
| RatecodeID           | double     | Tipo de tarifa              |
| store_and_fwd_flag   | string     | Flag de armazenamento       |
| PULocationID         | bigint     | Pickup Location ID          |
| DOLocationID         | bigint     | Dropoff Location ID         |
| payment_type         | double     | Tipo de pagamento           |
| fare_amount          | double     | Tarifa                      |
| extra                | double     | Extra                       |
| mta_tax              | double     | Taxa MTA                    |
| tip_amount           | double     | Gorjeta                     |
| tolls_amount         | double     | Pedágio                     |
| improvement_surcharge| double     | Taxa de melhoria            |
| total_amount         | double     | Valor total                 |
| congestion_surcharge | double     | Taxa de congestionamento    |
| airport_fee          | double     | Taxa de aeroporto           |
| ingestion_ts         | timestamp  | Timestamp de ingestão       |

**Particionamento:** `ano INT`, `mes INT`

### Bronze — Taxi Green

| Coluna                | Tipo       | Observação                  |
|-----------------------|-----------|-----------------------------|
| VendorID              | bigint     | Identificador do fornecedor |
| lpep_pickup_datetime  | timestamp  | Início da viagem            |
| lpep_dropoff_datetime | timestamp  | Fim da viagem               |
| store_and_fwd_flag    | string     | Flag de armazenamento       |
| RatecodeID            | double     | Tipo de tarifa              |
| PULocationID          | bigint     | Pickup Location ID          |
| DOLocationID          | bigint     | Dropoff Location ID         |
| passenger_count       | double     | Número de passageiros       |
| trip_distance         | double     | Distância da viagem         |
| fare_amount           | double     | Tarifa                      |
| extra                 | double     | Extra                       |
| mta_tax               | double     | Taxa MTA                    |
| tip_amount            | double     | Gorjeta                     |
| tolls_amount          | double     | Pedágio                     |
| ehail_fee             | string     | Taxa de chamada eletrônica  |
| improvement_surcharge | double     | Taxa de melhoria            |
| total_amount          | double     | Valor total                 |
| payment_type          | double     | Tipo de pagamento           |
| trip_type             | double     | Tipo de corrida             |
| congestion_surcharge  | double     | Taxa de congestionamento    |
| ingestion_ts          | timestamp  | Timestamp de ingestão       |

**Particionamento:** `ano INT`, `mes INT`

### Silver — Corridas Padronizadas

| Coluna               | Tipo       | Observação                     |
|----------------------|-----------|--------------------------------|
| id_fornecedor        | bigint     | VendorID                        |
| quantidade_passageiros| int       | passenger_count convertido para int |
| valor_total          | double     | total_amount                     |
| data_inicio_viagem   | timestamp  | tpep/lpep pickup datetime       |
| data_fim_viagem      | timestamp  | tpep/lpep dropoff datetime      |
| tipo_taxi            | string     | 'yellow' ou 'green'             |

**Particionamento:** `ano INT`, `mes INT`

### Gold - Visões/Materializações de Negócio
- `dados_taxi_gold.media_passageiros_hora_maio` — média de passageiros por hora (mês = 5)
- `dados_taxi_gold.media_valor_total_mes_yellow` — média do valor_total por ano/mes para táxis amarelos
---

## Arquivos Importantes no Repositório

- `src/glue/processamento_dados_bronze.py` — pipeline Bronze  
- `src/glue/processamento_dados_silver.py` — pipeline Silver  
- `glue/processamento_glue_notebook.ipynb` — notebook Glue interativo  
- `src/ddl/scripts_ddl.sql` — DDLs Bronze/Silver/Gold  
- `lambda/handler_bronze.py` — Lambda de ingestão exemplo  

---

## Como Rodar (Passo-a-Passo)

1. **Preparar permissões**  
   - S3: `GetObject`, `PutObject`, `DeleteObject`  
   - Glue: `CreateTable`, `UpdateTable`, `GetPartitions`  
   - Athena: `StartQueryExecution`, `GetQueryExecution`  
   - CloudWatch Logs

2. **Criar tabelas no catálogo**
   - No console Athena cole/execute o conteúdo de `src/ddl/scripts_ddl.sql` para criar as tabelas Bronze/Silver/Gold.

3. **Executar Bronze**
   - Opção A (Glue job): crie um Glue Job (Worker Type conforme necessidade) usando `src/glue/processamento_dados_bronze.py`. No job settings, informe `NumberOfWorkers` e o role correto.
   - Opção B (Glue Notebook): abra `glue/processamento_glue_notebook.ipynb` no Glue Studio e execute célula-a-célula.

4. **Executar Silver**
    - Use `src/glue/processamento_dados_silver.py` como Glue Job, ou execute o bloco Silver do notebook.
    
5. **Gerar Gold (materializações)**
    - Execute os blocos SQL do final de `src/ddl/scripts_ddl.sql` no Athena para criar as tabelas/materializações em S3.

6. **Validar resultados**
    - Verificar S3:
        - s3://ifood-case-bronze/
        - s3://ifood-case-silver/
        - s3://ifood-case-gold/
    - Rode queries em Athena sobre `dados_taxi_silver.corridas_taxi` para validar colunas e particionamento.
    - Rode queries para gerar camada gold com a resposta final das perguntas
    - Arquivos CSV disponíveis na pasta analysis

7. **Evidencias**
    - As evidências de todas execuções e dados estão em analysis -> evidencias

## Cuidados Operacionais e Troubleshooting

### HIVE_BAD_DATA / Parquet schema mismatch
- Causa comum: tipos no arquivo Parquet de origem não batem com os definidos no Glue/Athena (ex.: RatecodeID como INT64 vs DOUBLE no catálogo). Arquivos da mesma fonte de dicionário de dados, estava com tipos divergentes
- Solução recomendada: aplicar cast explícito antes de gravar. Os scripts e notebook já contêm helpers que tentam aplicar casts para o StructType definido; garanta que esta etapa rode antes de gravar.
- Para partições já escritas com tipos errados, apague os objetos S3 da partição e reexecute o job corrigido.

### Idempotência de partições:
    - Os jobs implementam escrita por partição. Em alguns pontos optei por `append` e em outros por reescrever partições; escolha a estratégia conforme volume e política de reprocessamento.
---

## Permissões Necessárias

- **S3:** `GetObject`, `PutObject`, `DeleteObject`, `ListBucket`  
- **Glue:** AWSGlueServiceRole ou equivalente  
- **Athena:** `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`  
- **CloudWatch Logs:** enviar logs

---

## Próximos Passos / Melhorias
- Implementar testes unitários & integração local com conjunto amostral de Parquet (moto/localstack para S3 mocks) para validar o pipeline antes de rodar em produção.
- Deixar parametrizavel fonte dos dados Landing (catalogar de acordo com data)
- Automatizar deploy dos Glue Jobs e DDLs via IaC (CloudFormation / CDK / Terraform).

