-- DDL de Criação de Tabelas para o Bronze 
CREATE EXTERNAL TABLE IF NOT EXISTS dados_taxi_bronze.taxi_yellow (
  VendorID bigint,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count bigint,
  trip_distance double,
  RatecodeID double,
  store_and_fwd_flag string,
  PULocationID bigint,
  DOLocationID bigint,
  payment_type double,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double,
  congestion_surcharge double,
  airport_fee double,
  ingestion_ts timestamp
)
PARTITIONED BY (
  ano int,
  mes int
)
STORED AS PARQUET
LOCATION 's3://ifood-case-bronze/taxi_yellow/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS dados_taxi_bronze.taxi_green (
  VendorID bigint,
  lpep_pickup_datetime timestamp,
  lpep_dropoff_datetime timestamp,
  store_and_fwd_flag string,
  RatecodeID double,
  PULocationID bigint,
  DOLocationID bigint,
  passenger_count double,
  trip_distance double,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  ehail_fee string,
  improvement_surcharge double,
  total_amount double,
  payment_type double,
  trip_type double,
  congestion_surcharge double,
  ingestion_ts timestamp
)
PARTITIONED BY (
  ano int,
  mes int
)
STORED AS PARQUET
LOCATION 's3://ifood-case-bronze/taxi_green/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- DDL de Criação de Tabela Silver unificada de Táxis (Amarelo + Verde)
CREATE EXTERNAL TABLE IF NOT EXISTS dados_taxi_silver.corridas_taxi (
  id_fornecedor bigint,                     -- VendorID
  quantidade_passageiros int,               -- passenger_count (convertido para inteiro)
  valor_total double,                       -- total_amount
  data_inicio_viagem timestamp,               -- tpep_pickup_datetime
  data_fim_viagem timestamp,                  -- tpep_dropoff_datetime
  tipo_taxi string                          -- 'yellow' ou 'green'
)
PARTITIONED BY (
  ano int,
  mes int
)
STORED AS PARQUET
LOCATION 's3://ifood-case-silver/corridas_taxi/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

--DDL de Criação Tabelas Gold
CREATE TABLE IF NOT EXISTS dados_taxi_gold.media_valor_total_mes_yellow
WITH (
    format = 'PARQUET',
    external_location = 's3://ifood-case-gold/media_valor_total_mes_yellow/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    ano,
    mes,
    AVG(valor_total) AS media_valor_total
FROM dados_taxi_silver.corridas_taxi
WHERE tipo_taxi = 'yellow'
GROUP BY ano, mes
ORDER BY ano, mes;

CREATE TABLE IF NOT EXISTS dados_taxi_gold.media_passageiros_hora_maio
WITH (
    format = 'PARQUET',
    external_location = 's3://ifood-case-gold/media_passageiros_hora_maio/',
    parquet_compression = 'SNAPPY'
) AS
SELECT
    HOUR(data_inicio_viagem) AS hora_do_dia,
    AVG(quantidade_passageiros) AS media_passageiros
FROM dados_taxi_silver.corridas_taxi
WHERE mes = 5
GROUP BY HOUR(data_inicio_viagem)
ORDER BY hora_do_dia;
