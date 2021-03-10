## Overview 
Data Modeling for "Sparkify" an online music streaming platform. The startup was collecting data on songs and user activity on the streaming platform.

They have grown their user database and song database and now they want to move their processes and data onto the cloud AWS.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, using Spark and ETL pipeline we will  create a data lake that automates the entire dataset by combining multiple data sources from AWS S3 into structured data. Also, the structured data then gets stored on AWS S3 again, so Sparkifys analytics team can use it for getting tons of insights from the available json format data.

### Song Dataset 
We will be working with two datasets Song dataset and Log dataset that reside in S3. Their desciption is provided below :

#### Song Dataset: 
It is a subset of real data from Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

#### Log Dataset:
This dataset consists of log files in JSON format generated by this  event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.The log files in the dataset are partitioned by year and month. 

### Input data location

The input data contains two S3 buckets:
* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`

### Generated tables

| name | type | description | columns |
| ---- | ---- | ----------- | ------- |
| songplays | fact table | records in log data associated with song plays i.e. records with page NextSong | songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent |
| users | dimension table | users in the app | user_id, first_name, last_name, gender, level |
| songs | dimension table | songs in music database | song_id, title, artist_id, year, duration |
| artists | dimension table |  artists in music database | artist_id, name, location, lattitude, longitude |
| time | dimension table | timestamps of records in songplays broken down into specific units | start_time, hour, day, week, month, year, weekday |

### Schema for Song Play Analysis

#### Fact Table
##### songplays
       songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables 
##### users
    user_id, first_name, last_name, gender, level
##### songs
    song_id, title, artist_id, year, duration
##### artists
    artist_id, name, location, lattitude, longitude
##### time
    start_time, hour, day, week, month, year, weekday

### Project files
There are 4 files in this project.
1. dl.cfg is the file that conatins AWS credentials.
2. local_etl.py is the ETL script that can used to run locally with small datset provided in workscpace.
3. etl.py is the ETL Script which takes input data from S3 and then stores the output parquet file of etl.py back to S3.
4. Readme.md is the Documentation regarding the project.


### To Run the project
1. To run the project, all the above files should be in a single Directory.
2. Create AWS IAM role with S3 read and write access.Setup the dl.cfg file with required credentials.
3. Create an S3 bucket and enter the URL to the bucket in `etl.py` as the value of output_data
4. Test by running etl.py script by typing "python etl.py" in terminal.
