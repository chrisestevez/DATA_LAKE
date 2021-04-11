import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Creates Spark session.

    Returns:
        obj: Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song data and converts to tables.

    Args:
        spark (obj): Spark session.
        input_data (str): S3 data location
        output_data (str): S3 output data location
    """
    # get filepath to song data file
    song_data = input_data +  "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data) 

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year",\
        "duration").dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
        .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name"\
        , "artist_location as location", "artist_latitude as latitude"\
            , "artist_longitude as longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data\
        , "artists/artists.parquet"), "overwrite")


def process_log_data(spark, input_data, output_data):
    """Process log data and convert to table.

    Args:
        spark (obj): Spark session.
        input_data (str): Input data.
        output_data (str): Output data location.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id"\
        , "firstName as first_name", "lastName as last_name"\
            , "gender", "level").where("userId IS NOT NULL")\
                .dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data\
        , "users/users.parquet"), "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df["ts"]))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn("datetime", get_datetime(df["ts"]))
    
    # extract columns to create time table
    time_table = df.selectExpr("datetime as start_time"\
        ,"hour(datetime) as hour"\
            ,"day(datetime) as day"\
                ,"weekofyear(datetime) as week"\
                    ,"month(datetime) as month"\
                        ,"year(datetime) as year"\
                            ,"dayofweek(datetime) as weekday")\
                                .dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data +  "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)
    
    song_df = song_df.select("song_id", "title","artist_id", "artist_name"\
        , "duration").dropDuplicates(["song_id"])

    # extract columns from joined song and log datasets to create songplays table 
     
                
    songplays_table = song_df.join(df, (song_df.title==df.song)\
        & (song_df.artist_name==df.artist)\
            & (song_df.duration==df.length), how = "inner")\
                .selectExpr("monotonically_increasing_id() as songplay_id"\
                    , "datetime as start_time"\
                        ,"year(datetime) as year"\
                            , "month(datetime) as month"\
                                ,"userId as user_id"\
                                    ,"level", "song_id", "artist_id"\
                                        , "sessionId as session_id"\
                                            ,"location"\
                                                ,"userAgent as user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'songplays/songplays.parquet')\
            , 'overwrite')


def main():
    """Executes project steps.
    """
    spark = create_spark_session()
    input_data = config.get('S3','INPUT')
    output_data = config.get('S3','OUTPUT')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
