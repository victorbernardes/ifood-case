import sys
import traceback
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
import boto3

# Inicialização do job no Glue
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Schema Yellow Taxi
yellow_schema = T.StructType([
    T.StructField("VendorID", T.LongType(), True),
    T.StructField("tpep_pickup_datetime", T.TimestampType(), True),
    T.StructField("tpep_dropoff_datetime", T.TimestampType(), True),
    T.StructField("passenger_count", T.LongType(), True),
    T.StructField("trip_distance", T.DoubleType(), True),
    T.StructField("RatecodeID", T.LongType(), True),
    T.StructField("store_and_fwd_flag", T.StringType(), True),
    T.StructField("PULocationID", T.LongType(), True),
    T.StructField("DOLocationID", T.LongType(), True),
    T.StructField("payment_type", T.LongType(), True),
    T.StructField("fare_amount", T.DoubleType(), True),
    T.StructField("extra", T.DoubleType(), True),
    T.StructField("mta_tax", T.DoubleType(), True),
    T.StructField("tip_amount", T.DoubleType(), True),
    T.StructField("tolls_amount", T.DoubleType(), True),
    T.StructField("improvement_surcharge", T.DoubleType(), True),
    T.StructField("total_amount", T.DoubleType(), True),
    T.StructField("congestion_surcharge", T.DoubleType(), True)
])

# Schema Green Taxi
green_schema = T.StructType([
    T.StructField("VendorID", T.LongType(), True),
    T.StructField("lpep_pickup_datetime", T.TimestampType(), True),
    T.StructField("lpep_dropoff_datetime", T.TimestampType(), True),
    T.StructField("store_and_fwd_flag", T.StringType(), True),
    T.StructField("RatecodeID", T.LongType(), True),
    T.StructField("PULocationID", T.LongType(), True),
    T.StructField("DOLocationID", T.LongType(), True),
    T.StructField("passenger_count", T.LongType(), True),
    T.StructField("trip_distance", T.DoubleType(), True),
    T.StructField("fare_amount", T.DoubleType(), True),
    T.StructField("extra", T.DoubleType(), True),
    T.StructField("mta_tax", T.DoubleType(), True),
    T.StructField("tip_amount", T.DoubleType(), True),
    T.StructField("tolls_amount", T.DoubleType(), True),
    T.StructField("improvement_surcharge", T.DoubleType(), True),
    T.StructField("total_amount", T.DoubleType(), True),
    T.StructField("payment_type", T.LongType(), True),
    T.StructField("congestion_surcharge", T.DoubleType(), True)
])

# Função para converter explicitamente os campos conforme schema
def cast_columns(df, table_name):
    if "taxi_yellow" in table_name:
        datetime_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        long_cols = ["VendorID", "passenger_count", "PULocationID", "DOLocationID"]
        double_cols = ["trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount","RatecodeID", "payment_type", "trip_type",
                       "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge"]
        string_cols = ["store_and_fwd_flag"]
    else:  # taxi_green
        datetime_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
        long_cols = ["VendorID",  "PULocationID", "DOLocationID", "passenger_count"]
        double_cols = ["trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount","RatecodeID", "payment_type", "trip_type",
                       "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge"]
        string_cols = ["store_and_fwd_flag", "ehail_fee"]

    for col_name in datetime_cols:
        df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
    for col_name in long_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(T.LongType()))
    for col_name in double_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))
    for col_name in string_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(T.StringType()))
    return df

# Função para listar arquivos no S3
def list_s3_files(bucket_path):
    s3 = boto3.client('s3')
    bucket = bucket_path.replace("s3://", "").split("/")[0]
    prefix = "/".join(bucket_path.replace("s3://", "").split("/")[1:])
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(".parquet"):
                files.append(f"s3://{bucket}/{obj['Key']}")
    return files

# Função principal de processamento arquivo por arquivo
def process_dataset(input_path: str, output_path: str, table_name: str, schema):
    print(f"\nIniciando processamento do dataset: {table_name}")
    try:
        files = list_s3_files(input_path)
        if not files:
            print(f"Nenhum arquivo encontrado em {input_path}")
            return

        for file in files:
            print(f"Processando arquivo: {file}")
            df = spark.read.parquet(file)  # leitura sem inferência de schema
            df = cast_columns(df, table_name)
            df = df.withColumn("source_file", F.lit(file))
            df = df.withColumn("ano", F.regexp_extract(F.col("source_file"), r"(\d{4})-(\d{2})", 1).cast(T.IntegerType())) \
                   .withColumn("mes", F.regexp_extract(F.col("source_file"), r"(\d{4})-(\d{2})", 2).cast(T.IntegerType())) \
                   .withColumn("ingestion_ts", F.current_timestamp()) \
                   .drop("source_file")

            # Sobrescrever as partições existentes
            df.write.mode("overwrite") \
                  .format("parquet") \
                  .partitionBy("ano", "mes") \
                  .option("compression", "snappy") \
                  .save(output_path)

        # Atualiza partições no Glue Catalog
        spark.sql(f"MSCK REPAIR TABLE {table_name}")
        print(f"Processamento finalizado com sucesso para {table_name}")

    except Exception as e:
        print("--------------------------------------------------")
        print(f"Erro ao processar {table_name}")
        print(f"Input: {input_path}")
        print(f"Destino: {output_path}")
        print("Mensagem:", str(e))
        print("Traceback completo:")
        traceback.print_exc()
        print("--------------------------------------------------")

# Configuração dos datasets
datasets = [
    {
        "input": "s3://ifood-case-repo/yellow/",
        "table": "dados_taxi_bronze.taxi_yellow",
        "output": "s3://ifood-case-bronze/taxi_yellow/",
        "schema": yellow_schema
    },
    {
        "input": "s3://ifood-case-repo/green/",
        "table": "dados_taxi_bronze.taxi_green",
        "output": "s3://ifood-case-bronze/taxi_green/",
        "schema": green_schema
    }
]

# Execução principal
for ds in datasets:
    process_dataset(ds["input"], ds["output"], ds["table"], ds["schema"])

job.commit()