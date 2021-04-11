import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Process the songs data files, extract data from songs, and write data into artists table.
    """

    song_data = "{0}song_data/*/*/*/*".format(input_data)

    df = spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").drop_duplicates()

    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year", "artist_id"])

    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                              "artist_longitude").drop_duplicates()

    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
        Process the event log file, and write data into time, users and songplays tables
    """

    log_data = os.path.join(input_data, "log-data/")

    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    df = df.filter(df.page == "NextSong")

    users_table = df.select("userId", "firstName", "lastName", "gender", "level").drop_duplicates()

    users_table.write.parquet(os.path.join(output_data, "users/"), mode="overwrite")

    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    time_table = df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time")) \
        .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year", "month"])

    song_df = spark.read.format("parquet") \
        .option("basePath", os.path.join(output_data, "songs/")).load(os.path.join(output_data, "songs/*/*/"))

    songplays_table = df.join(song_df, df.song == song_df.title, how='inner') \
        .select(monotonically_increasing_id().alias("songplay_id"), col("start_time"), col("userId").alias("user_id"),
                "level", "song_id", "artist_id", col("sessionId").alias("session_id"), "location",
                col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time,
                                           how="inner") \
        .select("songplay_id", songplays_table.start_time, "user_id",
                "level", "song_id", "artist_id", "session_id",
                "location", "user_agent", "year", "month")

    songplays_table.drop_duplicates() \
        .write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-spark-project/"
    output_data = "s3://udacity-spark-project/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
