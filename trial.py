import configparser
from datetime import datetime
import os
import pandas as pd
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))


os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')




spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()



    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
  

    # read log data file
    df = spark.read.json(log_data)
 # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'), 'gender', 'level')\
                            .dropDuplicates(['user_id', 'level'])

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table.parquet')
    print('File users_table.parquet created')
    
   
    