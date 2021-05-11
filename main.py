from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import json

spark = SparkSession \
    .builder \
    .appName("Cognitivo.ai Test") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Carregando arquivo de configuração de tipos de campos

json_file_path = "config/types_mapping.json"

with open(json_file_path, 'r') as j:
    contents = json.loads(j.read())

# Lendo arquivo CSV e atribuindo os tipos carregadores aos respectivos campos

df = spark.read.csv("data/input/users/load.csv", header=True)

df = df.withColumn('age', col('age').cast(contents['age']))
df = df.withColumn('create_date', col('create_date').cast(contents['create_date']))
df = df.withColumn('update_date', col('update_date').cast(contents['update_date']))

# Deduplicando dados, mantendo apenas a última atualização de cada ID

df = df.sort(desc("update_date"))
df = df.dropDuplicates(['id'])

# Escrevendo o resultado em formato parquet

df.write.parquet("data/output/load.parquet", mode='overwrite')
