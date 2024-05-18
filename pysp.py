import pyspark
from pyspark.sql import SparkSession

# Crie uma SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Crie um DataFrame simples
data = [("James", "Smith", "USA", 40), ("Anna", "Rose", "UK", 35)]
columns = ["firstname", "lastname", "country", "age"]
df = spark.createDataFrame(data, columns)

# Mostre o conteúdo do DataFrame
print(df)
# Pare a SparkSession
spark.stop()