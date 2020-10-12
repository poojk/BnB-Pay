import s3fs
from pyspark import SparkContext
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
    tablename=key.split("_")
    table = tablename[0]
    val = tablename[2].split(".")[0]
    path = f's3a://{bucket}/{key}'
    df = spark.read.option("header",True).csv(path)
    s = [s for s in df.columns if '2019' in s]
    selected = ['State','Metro'] + s
    df2=df.select(*selected)
    newdf = df2.withColumn('house_average', sum(df2[col] for col in s)/len(s))
    df1 = newdf.withColumn("house_average", F.round(newdf["house_average"], 1))
    d = df1.drop(*s)
    d = d.withColumn('bedrooms',F.lit(val))
    d = d.withColumnRenamed('Metro','city')
    d = d.dropna(subset=["city"])
    d = d.groupBy('state').agg(F.avg('house_average').alias('house_average'), F.first('bedrooms'))
    #d = d.withColumnRenamed('first(state)','state')
    d = d.withColumnRenamed('first(bedrooms)','bedrooms')
    d = d.withColumn("house_average", F.round(d["house_average"], 1))
    d = d.sort('state')
    d.show()
        d.write \
            .format("jdbc") \
            .option("url","jdbc:postgresql://10.0.0.8:5432/my_db") \
            .option("dbtable","house_prices") \
            .option("user","test") \
            .option("password","test") \
            .option("driver","org.postgresql.Driver") \
            .mode("Append") \
            .save()
