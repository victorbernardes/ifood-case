import sys
import traceback
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Inicialização do job no Glue
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Função para processar cada tabela Bronze
def process_bronze_table(input_table: str, taxi_type: str):
    print(f"\nProcessando tabela Bronze: {input_table}")
    try:
        df = spark.table(input_table)
        
        if taxi_type == "yellow":
            df_silver = df.select(
                F.col("VendorID").alias("id_fornecedor"),
                F.col("passenger_count").cast(T.IntegerType()).alias("quantidade_passageiros"),
                F.col("total_amount").alias("valor_total"),
                F.col("tpep_pickup_datetime").alias("data_inicio_viagem"),
                F.col("tpep_dropoff_datetime").alias("data_fim_viagem"),
                F.lit("yellow").alias("tipo_taxi"),
                F.col("ano"),
                F.col("mes")
            )
        else:  # green
            df_silver = df.select(
                F.col("VendorID").alias("id_fornecedor"),
                F.col("passenger_count").cast(T.IntegerType()).alias("quantidade_passageiros"),
                F.col("total_amount").alias("valor_total"),
                F.col("lpep_pickup_datetime").alias("data_inicio_viagem"),
                F.col("lpep_dropoff_datetime").alias("data_fim_viagem"),
                F.lit("green").alias("tipo_taxi"),
                F.col("ano"),
                F.col("mes")
            )

        return df_silver

    except Exception as e:
        print("--------------------------------------------------")
        print(f"Erro ao processar {input_table}")
        print("Mensagem:", str(e))
        print("Traceback completo:")
        traceback.print_exc()
        print("--------------------------------------------------")
        return None

# Lista de tabelas Bronze
bronze_tables = [
    {"table": "dados_taxi_bronze.taxi_yellow", "tipo": "yellow"},
    {"table": "dados_taxi_bronze.taxi_green", "tipo": "green"}
]

# Processa e concatena os datasets
df_silver_all = None
for bt in bronze_tables:
    df_silver = process_bronze_table(bt["table"], bt["tipo"])
    if df_silver is not None:
        if df_silver_all is None:
            df_silver_all = df_silver
        else:
            df_silver_all = df_silver_all.unionByName(df_silver)

# Caminho de destino Silver
output_silver_path = "s3://ifood-case-silver/corridas_taxi/"

# Escreve a tabela Silver particionada
if df_silver_all is not None:
    df_silver_all.write.mode("overwrite") \
        .format("parquet") \
        .partitionBy("ano", "mes") \
        .option("compression", "snappy") \
        .save(output_silver_path)

    # Atualiza partições no Glue Catalog
    spark.sql("MSCK REPAIR TABLE dados_taxi_silver.corridas_taxi")
    print("Ingestão para Silver finalizada com sucesso.")

job.commit()