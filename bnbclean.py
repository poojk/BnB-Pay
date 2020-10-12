from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql import DataFrame

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/usr/local/spark/postgresql-42.2.16.jar") \
        .getOrCreate()

df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.0.8:5432/my_db") \
        .option("dbtable", "airbnb") \
        .option("user", "test") \
        .option("password", "test") \
        .option("driver", "org.postgresql.Driver") \
        .load()

df = df.withColumn('bedrooms',F.round(df['bedrooms'],0))
df = df.filter(~F.col('city').contains("/"))
df = df.filter(~F.col('city').contains(","))
df = df.filter(~F.col('city').contains("-"))
df = df.filter(~F.col('city').contains("^[0-9]*$"))
df = df.filter(~F.col('city').contains("*"))
df = df.withColumn('city', F.ltrim(df.city))
df = df.withColumn("city", F.initcap(F.col("city")))
df = df.filter(~df.city.rlike("[ ,;{}()\n\t=]"))
df = df.filter(~df.city.rlike("[^0-9A-Za-z]"))
df = df.filter(~F.col('city').contains("("))
df = df.groupBy('city','bedrooms').agg(F.avg('average').alias('average'),F.first('state'))
df = df.withColumnRenamed('first(state)','state')
df = df.sort('city')
df1 = df.withColumn('average', F.round(df['average'], 0))

df1.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.0.8:5432/my_db") \
        .option("dbtable", "bnbclean") \
        .option("user", "test") \
        .option("password", "test") \
        .option("driver", "org.postgresql.Driver") \
        .mode("Overwrite") \
        .save()
