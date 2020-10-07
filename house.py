from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import boto3
from array import array

year = 2019
region = 'us-east-2'
bucket = 'ins-de-proj'
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('ins-de-proj')
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

for my_bucket_object in my_bucket.objects.all():
    key = my_bucket_object.key
    tablename=key.split(".")
    table = tablename[0]
    path = f's3a://{bucket}/{key}'
    df = spark.read.option("header",True).csv(path)
    s = [s for s in df.columns if '2019' in s]
    selected = ['State','Metro','RegionName'] + [s for s in df.columns if '2019' in s]
    df2=df.select(*selected)
    newdf = df2.withColumn('average', sum(df2[col] for col in s)/len(s))
    df1 = newdf.withColumn("average", F.round(newdf["average"], 1))
    #newdf.show()

    df1.write \
            .format("jdbc") \
            .option("url","jdbc:postgresql://10.0.0.8:5432/my_db") \
            .option("dbtable",table) \
            .option("user","test") \
            .option("password","test") \
            .option("driver","org.postgresql.Driver") \
            .mode("Append") \
            .save()
