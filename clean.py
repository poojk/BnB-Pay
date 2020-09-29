import io
from io import StringIO
import os
import boto3
import pandas as pd
import s3fs
import numpy as np

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('airbnbcrawl')
s3_path = 's3://airbnbcrawl/'

year = 2019 ### skipping 2015 as it lacks info on number of reviews
year_file_list = []
for my_bucket_object in my_bucket.objects.all():
    current_file = my_bucket_object.key
    current_list = current_file.split('_')
    current_city = current_list[0]
    date = current_list[1].split('-')
    current_year = int(date[0])
    current_month = int(date[1])
    if current_year == year:
        year_file_list.append(my_bucket_object.key)

cleaned_bucket = s3.Bucket('bnb2019cleaned')
s3_path_cleaned = 's3://bnb2019cleaned/'
for file_names in year_file_list:
    print("Trying to upload:",file_names)
    current_file = file_names
    current_list = current_file.split('_')
    date = current_list[1].split('-')
    current_year = int(date[0])
    current_month = int(date[1])
    ### Extracting only the required columnsi
    df = pd.read_csv(os.path.join(s3_path,file_names),usecols =["id", "neighbourhood", "city", "state", "smart_location", "bedrooms", "price"])
    ### Data quality check - filter out listings with no valid ID, price, reviews per month
    df[df['price'].str.strip().astype(bool)]
    df['Year'] = current_year
    df['Month'] = current_month
    #Writing to S3 using BOTO3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_cleaned = boto3.resource('s3')
    s3_cleaned.Object('bnb2019cleaned',file_names).put(Body=csv_buffer.getvalue())
                                                                                                                                                          45,1          Bot 
