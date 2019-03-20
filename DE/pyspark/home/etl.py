import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id','title','artist_id','artist_name','year','duration']].drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_name").mode('overwrite').parquet("songs.parquet")

    # extract columns to create artists table
    artists_table = df[['artist_id','artist_name',
                  'artist_location','artist_latitude','artist_longitude']].drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("artists.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+'/log_data/*.csv'

    # read log data file
    df = spark.read.csv(log_data,header=True)
    print("Log Files Read")
    print(df.count())    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df[['userId','firstName','lastName','gender','level']].drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("users.parquet")

    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)
       
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    df = df.withColumn("datetime", get_timestamp(df.ts))
        
    # extract columns to create time table
    time_table = df.select('ts','datetime','timestamp',
                           year(df.datetime).alias('year'),
                           month(df.datetime).alias('month')
                          ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet("time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(song_df.title==df.song)&(song_df.artist_name==df.artist)&(song_df.duration==df.length),
                              how='inner')
    print("Merge Completed")
    print(songplays_table.count())
    
    print("Check Merge Rows above ^^^^^")
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn("month", month(songplays_table.datetime).alias('month'))   
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet("songplays.parquet")
    

def main():
    spark = create_spark_session()
    input_data = "s3a://dend"
    output_data = "s3a://dend/analytics"
    
    print("Starting Processing")
    process_song_data(spark, input_data, output_data)  
    print("Processed Song Data")
    process_log_data(spark, input_data, output_data)
    print("Processed Log Data")


if __name__ == "__main__":
    main()
