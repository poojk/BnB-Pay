from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = 'bnb2019cleaned'
#s3_path = 's3://bnb2019cleaned/'
my_bucket = s3.Bucket('bnb2019cleaned')
region = 'us-east-2'
#key = 'asheville_2019-01-27_listings.csv'

sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

for my_bucket_object in my_bucket.objects.all():
    key = my_bucket_object.key
    path = f's3a://{bucket}/{key}'
    df = spark.read.option("header",True).csv(path)
    new = df.withColumn('price', F.regexp_replace(col('price'), '\\$', ''))
    agg_df = new.groupBy('city','bedrooms',).agg(F.avg('price').alias('average'),F.first('state') ,F.first('month'))
    agg_df = agg_df.withColumnRenamed('first(state)','state')
    agg_df = agg_df.withColumnRenamed('first(month)','month')
    print("Trying to upload:",key)
    agg_df = agg_df.dropna(subset=["average"])
    df1 = agg_df.withColumn("average", F.round(agg_df["average"], 1))
    df1.sort('city')
    df1.write \
            .format("jdbc") \
            .option("url","jdbc:postgresql://10.0.0.8:5432/my_db") \
            .option("dbtable","airbnb") \
            .option("user","test") \
            .option("password","test") \
            .option("driver","org.postgresql.Driver") \
            .mode("Append") \
            .save()
