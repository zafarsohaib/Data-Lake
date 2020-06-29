import configparser
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("Test Spark") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    #song_data = os.path.join(input_data, "song_data/*/*/*/*/*.json")
                             
    # read song data file
    df = spark.read.json(song_data)
    
    # Song Table Temporary View
    df.createOrReplaceTempView("song_table")
    
    # extract columns to create songs table
    songs_table = spark.sql(""" 
        select distinct song_id, title, artist_id, year, duration 
        from song_table    
        """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs"), mode='overwrite')

    # extract columns to create artists table
    artists_table = spark.sql(""" 
        select distinct artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
        from  ( select artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
                row_number() over (partition by artist_id order by artist_name) as rown
                from song_table
                where artist_id is not NULL )
        where rown=1 
        """)

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists"), mode='overwrite')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    #log_data = os.path.join(input_data, "log_data/*.json")
    
    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    df.createOrReplaceTempView("log_table")

    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table    
    users_table = spark.sql("""
    select distinct userId as user_id, firstName as first_name, lastName as last_name, gender, level
        from ( select userId, firstName, lastName, gender, level,
                row_number() over (partition by userId order by ts desc) as rown
                from log_table
                WHERE page = 'NextSong' AND userId <> '')
        where rown=1
    """)

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users"), mode='overwrite')

    # create timestamp column from original timestamp column
    
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    df.createOrReplaceTempView("log_table")
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
                .withColumn("day", dayofmonth("start_time")) \
                .withColumn("week", weekofyear("start_time")) \
                .withColumn("month", month("start_time")) \
                .withColumn("year", year("start_time")) \
                .withColumn("weekday", dayofweek("start_time"))\
                .withColumn("pk_year", year("start_time"))\
                .withColumn("pk_month", month("start_time")) \
                .select(["start_time", "hour", "day", "week", "month", "year", "weekday", "pk_year", "pk_month"]).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time"), mode='overwrite', partitionBy=["pk_year", "pk_month"])

    # extract columns from joined song and log datasets to create songplays table
    # year and month columns added for partitioning 
    songplays_table = spark.sql("""
            select distinct t1.start_time, t1.userId as user_id, t1.level, t2.song_id, 
            t2.artist_id, t1.sessionId as session_id, t1.location, t1.userAgent as user_agent
            from log_table t1
            inner join song_table t2 on (t2.artist_name = t1.artist AND t2.title = t1.song)
            where t1.page = 'NextSong'
            """) \
        .withColumn("songplay_id", F.monotonically_increasing_id())\
        .withColumn("pk_year", year("start_time"))\
        .withColumn("pk_month", month("start_time"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data,"songplay"), mode='overwrite', partitionBy = ["pk_year", "pk_month"])

def main():
    spark = create_spark_session()

    #input_data = "data/"
    #output_data = "output/"

    input_data = "s3://udacity-dend/"
    output_data = "s3://zafarsohaib/output"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()
