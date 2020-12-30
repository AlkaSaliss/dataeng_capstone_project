from functools import reduce
import os
import time

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, StringType, StructType, TimestampType


def _print_format(msg): print("-"*80, "\t"+msg, "-"*80, sep="\n")


def create_spark_session():
    """Creates a spark session if it doesn't exist and returns it

    Returns:
        SparkSession: spark session
    """
    spark = SparkSession \
        .builder \
        .appName("LOPRO2TS") \
        .getOrCreate()
    return spark


def load_csv_spark(data_path, spark, columns_mapping, schema, fill_na):
    df = spark.read.csv(data_path, sep=";", header=True, schema=schema)
    df = df\
        .select([F.col(c).alias(columns_mapping.get(c, c)) for c in df.columns])\
        .fillna(fill_na)
    return df


def load_json_spark(data_path, spark, list_expr, fill_na):
    return spark.read.json(data_path)\
        .selectExpr(*list_expr)\
        .fillna(fill_na)


def process_fact_and_dim_tables(df_loss_declared: pyspark.sql.dataframe.DataFrame, df_declared_and_found: pyspark.sql.dataframe.DataFrame,
                                df_stations: pyspark.sql.dataframe.DataFrame, df_temperature: pyspark.sql.dataframe.DataFrame, out_path):
    # create date columns from timestamp
    df_loss_declared = df_loss_declared.withColumn(
        "date", F.to_date(F.col("date_and_time")))
    df_declared_and_found = df_declared_and_found.withColumn(
        "date", F.to_date(F.col("date_and_time")))

    # join with stations
    df_loss_declared = df_loss_declared\
        .join(df_stations.select("station_id", "station_county_code"), "station_id", "inner")
    df_declared_and_found = df_declared_and_found\
        .join(df_stations.select("station_id", "station_county_code"), "station_id", "inner")

    # join with temperature
    df_loss_declared = df_loss_declared\
        .join(df_temperature, (df_loss_declared.station_county_code == df_temperature.county_code) & (df_loss_declared.date == df_temperature.date), "inner")
    df_declared_and_found = df_declared_and_found\
        .join(df_temperature, (df_declared_and_found.station_county_code == df_temperature.county_code) & (df_declared_and_found.date == df_temperature.date), "inner")

    # add delay in hours between declaration and recovery times
    diff_secs_col = F.col("recovery_date").cast(
        "long") - F.col("date_and_time").cast("long")
    df_declared_and_found = df_declared_and_found.withColumn(
        "delay_before_recovery", diff_secs_col/3600.0)

    # save loss declaration
    save_path = os.path.join(out_path, "loss_declaration")
    df_loss_declared\
        .withColumn("declaration_id", F.monotonically_increasing_id())\
        .select('declaration_id', 'date_and_time', 'station_id', 'property_nature', 'property_type',
                'min_temperature', 'max_temperature', 'avg_temperature')\
        .write\
        .partitionBy('station_id', 'property_type')\
        .parquet(save_path, mode="overwrite")

    # save declared and found properties
    save_path = os.path.join(out_path, "declared_and_found")
    df_declared_and_found\
        .withColumn("found_id", F.monotonically_increasing_id())\
        .select('found_id', 'date_and_time', 'station_id', 'property_nature', 'property_type', 'recovery_date',
                'delay_before_recovery', 'min_temperature', 'max_temperature', 'avg_temperature')\
        .write\
        .partitionBy('station_id', 'property_type')\
        .parquet(save_path, mode="overwrite")

    # save stations
    save_path = os.path.join(out_path, "stations")
    df_stations\
        .write\
        .parquet(save_path, mode="overwrite")

    # save time
    save_path = os.path.join(out_path, "time")
    reduce(pyspark.sql.DataFrame.unionAll, [df_loss_declared.select("date_and_time"),
                                            df_declared_and_found.select("date_and_time"), df_declared_and_found.select("recovery_date")])\
        .distinct()\
        .withColumn("hour", F.hour(F.col("date_and_time")))\
        .withColumn("day", F.dayofmonth(F.col("date_and_time")))\
        .withColumn("week", F.weekofyear(F.col("date_and_time")))\
        .withColumn("month", F.month(F.col("date_and_time")))\
        .withColumn("year", F.year(F.col("date_and_time")))\
        .withColumn("weekday", F.date_format(F.col("date_and_time"), "E"))\
        .write\
        .partitionBy("year", "month")\
        .parquet(save_path, mode="overwrite")


def main():
    start = time.time()
    spark = create_spark_session()

    # define path to datasets
    path_declared_loss = "input_data/objets-trouves-gares.csv"
    path_declared_and_found = "input_data/objets-trouves-restitution.csv"
    path_stations = "input_data/referentiel-gares-voyageurs.json"
    path_temperature = "input_data/temperature-quotidienne-departementale.csv"

    _print_format("Loading loss declarations data")
    schema_lost = StructType()\
        .add("Date", TimestampType(), False)\
        .add("Gare", StringType(), True)\
        .add("Code UIC", StringType(), True)\
        .add("Nature d'objets", StringType(), False)\
        .add("Type d'objets", StringType(), False)\
        .add("Type d'enregistrement", StringType(), False)
    columns_mapping = dict(zip(
        ["Date", "Date et heure de restitution", "Gare", "Code UIC",
            "Nature d'objets", "Type d'objets", "Type d'enregistrement"],
        ["date_and_time", "recovery_date", "station_name", "station_id",
            "property_nature", "property_type", "recording_type"]
    ))
    fill_na_dict = {"station_name": "NOT_A_STATION",
                    "station_id": "NOT_A_STATION"}
    df_loss_declared = load_csv_spark(
        path_declared_loss, spark, columns_mapping, schema_lost, fill_na_dict)

    _print_format("Loading lost and found properties data")
    schema_found = StructType()\
        .add("Date", TimestampType(), False)\
        .add("Date et heure de restitution", TimestampType(), True)\
        .add("Gare", StringType(), True)\
        .add("Code UIC", StringType(), True)\
        .add("Nature d'objets", StringType(), False)\
        .add("Type d'objets", StringType(), False)\
        .add("Type d'enregistrement", StringType(), False)
    df_declared_and_found = load_csv_spark(
        path_declared_and_found, spark, columns_mapping, schema_found, fill_na_dict)

    _print_format("Loading temperatures data")
    schema_temp = StructType()\
        .add("Date", DateType(), True)\
        .add("Code INSEE département", StringType(), True)\
        .add("Département", StringType(), True)\
        .add("TMin (°C)", FloatType(), True)\
        .add("TMax (°C)", FloatType(), True)\
        .add("TMoy (°C)", FloatType(), True)
    columns_mapping = dict(zip(
        ["Date", "Code INSEE département", "Département",
            "TMin (°C)", "TMax (°C)", "TMoy (°C)"],
        ["date", "county_code", "county_name", "min_temperature",
            "max_temperature", "avg_temperature"]
    ))
    df_temperature = load_csv_spark(
        path_temperature, spark, columns_mapping, schema_temp, {})

    _print_format("Loading stations data")
    list_expr = ["fields.uic_code as station_id", "fields.alias_libelle_noncontraint as station_name", "cast(fields.latitude_entreeprincipale_wgs84 as float) as station_latitude",
                 "cast(fields.longitude_entreeprincipale_wgs84 as float) as station_longitude", "fields.commune_code as station_city_code", "fields.commune_libellemin as station_city_name",
                 "fields.departement_numero as station_county_code", "fields.departement_libellemin as station_county_name",
                 "fields.adresse_cp as station_postal_code", "cast(fields.gare_nbpltf as int) as station_number_of_platforms", "fields.segmentdrg_libelle as station_category"]
    df_stations = load_json_spark(
        path_stations, spark, list_expr, fill_na_dict)
    # add default row to address missing loss that didn't happen in stations
    df_default_station = spark.createDataFrame([("NOT_A_STATION", "NOT_A_STATION", -999, -999, "NOT_A_STATION",
                                                 "NOT_A_STATION", "NOT_A_STATION", "NOT_A_STATION", "NOT_A_STATION", -999, "NOT_A_STATION")], df_stations.columns)
    df_stations = df_stations.union(df_default_station)

    _print_format("Processing and saving fact and dimension tables")
    out_path = "output_data"
    process_fact_and_dim_tables(
        df_loss_declared, df_declared_and_found, df_stations, df_temperature, out_path)


if __name__ == "__main__":
    main()
