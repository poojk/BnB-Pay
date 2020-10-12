from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars", "/usr/local/spark/postgresql-42.2.16.jar") \
        .getOrCreate()

def read_table(name):
    df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://10.0.0.8:5432/my_db") \
            .option("dbtable", name) \
            .option("user", "test") \
            .option("password", "test") \
            .option("driver", "org.postgresql.Driver") \
            .load()
    return (df)

df = read_table("bnbclean")
df1 = read_table("house_prices")

def mortgage(house_price):
    rate = 3.3
    down_payment = (house_price * 30) / 100
    loan = house_price - down_payment
    interest = (loan * rate) / 100
    yearly_mortgage = (loan + interest) / 30
    monthly_mortgage = yearly_mortgage / 12
    return (monthly_mortgage)

def tax (house_value):
        tax = 2
        tax_amount = (house_value*tax)/100
        return (tax_amount)

def percent (x,y):
    p = (y/x)*100
    return (p)

da = df.join(df1, on=['state','bedrooms'], how='inner')
da = da.withColumn("tax", F.round(((tax(F.col('house_average')))/12),1))
da = da.withColumn("monthly_mortgage", F.round(mortgage(F.col('house_average'))+F.col('tax'),1))
da = da.withColumn("%", F.round(percent(F.col('monthly_mortgage'),F.col('average')),1))
d = da.distinct()

d.write \
        .format("jdbc") \
        .option("url","jdbc:postgresql://10.0.0.8:5432/my_db") \
        .option("dbtable","finali") \
        .option("user","test") \
        .option("password","test") \
        .option("driver","org.postgresql.Driver") \
        .save()
