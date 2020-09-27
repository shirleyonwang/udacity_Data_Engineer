import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType,DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Description: This function can be creat a spark sesion.

    Returns:
        spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to extract data from S3, load to spark dataframe.
    extract data to create tables: songs, artists

    Arguments:
        spark: spark session. 
        input_data: data source:S3
        output_data: the place to store result data

    Returns:
        None
    """
    songdata_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])
    
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/A/B")
    
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)
   
    # create view of songs, prepare for songplay
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = df[['year','artist_id','song_id','title','duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')
    
    # extract columns to create artists table
    artists_table = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')
    


def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to extract data from S3, load to spark dataframe.
    extract data to create tables: songs, artists

    Arguments:
        spark: spark session. 
        input_data: data source:S3
        output_data: the place to store result data

    Returns:
        None
    """
    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("ItemInSession", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", StringType(), True),
        StructField("song", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])
    
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/2018/11/*.json")

    # read log data file
    df = spark.read.json(log_data, schema=logdata_schema)
    #print(df.head(5))
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    user_table = df[['userId','firstName','lastName','gender','level']].drop_duplicates()
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column    
    get_timestamp = udf(lambda x: datetime.fromtimestamp((int(x)/1000.0)), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime= udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime('%Y-%m-%d'))
    df =  df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df[['ts',#'datetime','start_time',
                           hour(df.start_time).alias('hour'),
                           dayofmonth(df.start_time).alias('day'),
                           weekofyear(df.start_time).alias('week'),
                           month(df.start_time).alias('month'),
                           year(df.start_time).alias('year'),
                           #dayofweek (df.start_time).alias('weekday')
                    ]].dropDuplicates()
    
    #print(time_table.head())
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), 'overwrite')
  
    # read in song data to use for songplays table    
    df.createOrReplaceTempView("log_data")
    
    #song_df = spark.sql("select * from songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT DISTINCT                                      
                                      ld.ts,                                      
                                      ld.userId ,
                                      ld.level,
                                      sd.song_id,
                                      sd.artist_id,
                                      ld.sessionId ,
                                      ld.location,
                                      ld.userAgent
                                FROM log_data ld
                                JOIN songs_data sd ON 
                                    ld.artist = sd.artist_name
                                    and ld.song = sd.title
                                    and ld.length = sd.duration
                                """)    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), 'overwrite') 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"    
    output_data = "s3a://udacity-dend/" # "" for test, since S3a have no authorization
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
