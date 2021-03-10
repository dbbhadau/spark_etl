import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.types import TimestampType

from pyspark.sql.functions import *

# load config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    Create Spark session
    return: Spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process SONG data
    spark: Spark session
    input_data: location of the data
    output_data: location of the generated output (Parquet files)
    
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table.parquet')
    print('File songs_table.parquet created')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',          'artist_longitude').dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table.parquet')
    print('File artists_table.parquet created')


def process_log_data(spark, input_data, output_data):
    """
    Process LOG data
    :param spark: Spark session
    :param input_data: location of the data
    :param output_data: location of the generated output (Parquet files)
    """
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

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    # extract columns to create time table
    time_table = df.select(df.start_time.alias('start_time'), \
                hour(df.start_time).alias('hour'),\
                dayofmonth(df.start_time).alias('day'),\
                weekofyear(df.start_time).alias('week'),\
                month(df.start_time).alias('month'),\
                year(df.start_time).alias('year'),
                dayofweek(df.start_time).alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy(['year','month']).parquet(output_data+ 'time.parquet')    
    print('File time.parquet created')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    songs_df = spark.read.json(song_data)
    # created song view to write SQL Queries
    songs_df.createOrReplaceTempView("songs_table")

         
     # created log view to write SQL Queries
    log_data = input_data + 'log_data/*.json'
    log_df = spark.read.json(log_data)     
    log_df.createOrReplaceTempView("logs_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" select monotonically_increasing_id() as songplay_id, 
    timestamp(l.ts/1000) as start_time, 
    month(to_timestamp(l.ts/1000)) as month, 
    year(to_timestamp(l.ts/1000)) as year, 
    l.userId as user_id, l.level as level, 
    s.song_id as song_id, s.artist_id as artist_id, 
    l.sessionId as session_id, l.location as location, 
    l.userAgent as user_agent FROM 
    logs_table l JOIN songs_table s on l.artist = s.artist_name and l.song = s.title """)
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table.parquet')
    print('File songplays_table.parquet created')


def main():
    """
    Main: runs the pipeline
    """
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacity1-data-lake/ "

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()