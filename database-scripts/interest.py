import s3fs
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from array import array

region = 'us-east-2'
bucket = 'bnb2019cleaned'
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('bnb2019cleaned')
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)
key = "rates.csv"

path = f's3a://{bucket}/{key}'
df = spark.read.option("header",True).csv(path)
df1 = df.filter(df.observation_date.startswith('2019'))
d = df1.agg(avg(col("MORTGAGE15US")))
d.write \
        .format("jdbc") \
        .option("url","jdbc:postgresql://10.0.0.8:5432/my_db") \
        .option("dbtable","rate") \
        .option("user","test") \
        .option("password","test") \
        .option("driver","org.postgresql.Driver") \
        .mode("Append") \
        .save()
