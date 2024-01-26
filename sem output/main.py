from pyspark.sql import SparkSession
from pyspark import SparkConf

if __name__ == '__main__':
   spark = (SparkSession.builder
           .master("local[3]")
           .appName("estudando-Spark")
           .getOrCreate())

   # conf = SparkConf() \
   #  .setMaster("local") \
   # .setAppName("meu-app")

   # spark2 = SparkSession.builder.config(conf = conf).getOrCreate()



   print(spark)

   df = (spark
         .read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("delimiter", ";")
         .load(r"C:\Users\joth1\Documents\projetos\Forky-ArthurBarbero-Projeto\jony-arquivos\dados_acidentes_prf_2022.csv"))

   df.show()



