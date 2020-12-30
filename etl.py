from datetime import datetime
import time
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType


# helper function to pretty print
def _print_format(msg): print("-"*80, "\t"+msg, "-"*80, sep="\n")


def create_spark_session():
    """Creates a spark session if it doesn't exist and returns it

    Returns:
        SparkSession: spark session
    """
    spark = SparkSession \
        .builder \
        .appName("sparkify") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/")

    # read song data file
    _print_format("Loading song files ...")
    df = spark.read.json(song_data)

    # extract columns and cast to correct types, to create songs table
    _print_format(
        "Extracting songs columns and writing `songs` table to parquet files")
    list_expr_songs = ["song_id", "title", "artist_id",
                       "cast(year as int) as year", "cast(duration as float) as duration"]
    songs_table = df.selectExpr(*list_expr_songs)\
        .dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    song_outpath = os.path.join(output_data, "songs")
    song_partition_cols = ["year", "artist_id"]
    songs_table.write\
        .partitionBy(*song_partition_cols)\
        .parquet(song_outpath, mode="overwrite")

    # extract columns to create artists table
    _print_format(
        "Extracting artists columns and writing `artists` table to parquet files")
    list_expr_artists = ["artist_id", "artist_name", "artist_location",
                         "cast(artist_latitude as float) as latitude", "cast(artist_longitude as float) as longitude"]
    artists_table = df.selectExpr(*list_expr_artists)\
        .dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_outpath = os.path.join(output_data, "artists")
    artists_table\
        .write.parquet(artists_outpath, mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/")

    # read log data file
    _print_format("Loading song plays files ...")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    _print_format(
        "Extracting users columns and writing `users` table to parquet files")
    list_expr_users = ["userId as user_id", "firstName as first_name",
                       "lastName as last_name", "gender", "level", "ts"]
    # function to get a users's latest info using timestamp. 
    # for example a user may have "free" level at earlier date and upgrade to "paid" later
    get_latest_userindex = F.udf(lambda x: int(np.argmax(x)), IntegerType())
    users_table = df.selectExpr(*list_expr_users)\
        .groupBy("user_id")\
        .agg(F.collect_list("ts").alias("ts"),
             F.collect_list("first_name").alias("first_name"),
             F.collect_list("last_name").alias("last_name"),
             F.collect_list("gender").alias("gender"),
             F.collect_list("level").alias("level"))\
        .withColumn("latest_id", get_latest_userindex(F.col("ts")))
    
    list_users_cols = ["first_name", "last_name", "gender", "level"]
    for col_ in list_users_cols:
        get_latest_value = F.udf(lambda row: row[col_][row.latest_id])
        users_table = users_table.withColumn(col_, get_latest_value(F.struct([users_table[c] for c in [col_, "latest_id"]])) )

    # write users table to parquet files
    users_outpath = os.path.join(output_data, "users")
    users_table\
        .select("user_id", *list_users_cols)\
        .write.parquet(users_outpath, mode="overwrite")

    # create timestamp column from original timestamp column
    _print_format(
        "Extracting time columns and writing `time` table to parquet files")
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn("start_time", get_timestamp(F.col("ts")))


    # extract columns to create time table
    time_table = df.select("start_time")\
        .withColumn("hour", F.hour(F.col("start_time")))\
        .withColumn("day", F.dayofmonth(F.col("start_time")))\
        .withColumn("week", F.weekofyear(F.col("start_time")))\
        .withColumn("month", F.month(F.col("start_time")))\
        .withColumn("year", F.year(F.col("start_time")))\
        .withColumn("weekday", F.date_format(F.col("start_time"), "E"))

    # write time table to parquet files partitioned by year and month
    time_outpath = os.path.join(output_data, "time")
    time_partition_cols = ["year", "month"]
    time_table\
        .dropDuplicates(["start_time"])\
        .write\
        .partitionBy(*time_partition_cols)\
        .parquet(time_outpath, mode="overwrite")

    # read in song data to use for songplays table
    _print_format(
        "Extracting songplays columns and writing `songplays` table to parquet files")
    song_df = spark.read.parquet(os.path.join(output_data, "songs"))\
        .select("song_id", "title", "artist_id")

    # extract columns from joined song and log datasets to create songplays table
    # write songplays table to parquet files partitioned by year and month
    songplays_outpath = os.path.join(output_data, "songplays")
    df\
        .join(song_df, df.song == song_df.title)\
        .withColumnRenamed("userId", "user_id")\
        .withColumnRenamed("sessionId", "session_id")\
        .withColumnRenamed("userAgent", "user_agent")\
        .withColumn("songplay_id", F.monotonically_increasing_id())\
        .withColumn("month", F.month(F.col("start_time")))\
        .withColumn("year", F.year(F.col("start_time")))\
        .select("songplay_id", "start_time", "year", "month", "user_id", "level", "song_id",
                "artist_id", "session_id", "location", "user_agent")\
        .write\
        .partitionBy("year", "month")\
        .parquet(songplays_outpath, mode="overwrite")


def main():
    start = time.time()
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-dl-sp/sparkify-data/"

    process_song_data(spark, input_data, output_data)
    end1 = time.time()
    m, s = divmod(end1-start, 60)
    h, m = divmod(m, 60)
    _print_format(f"Processing songs data took {h}h - {m}mn - {s:.1f}s")

    process_log_data(spark, input_data, output_data)
    m, s = divmod(time.time()-end1, 60)
    h, m = divmod(m, 60)
    _print_format(f"Processing logs data took {h}h - {m}mn - {s:.1f}s")

if __name__ == "__main__":
    main()
