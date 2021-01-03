import os
import logging
import sys
from functools import reduce

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, FloatType, StringType, StructType,
                               TimestampType)


# setting the logger, taken from https://stackoverflow.com/a/28330410/7437524
logging.basicConfig(filename='app.log', format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
handler = logging.FileHandler('app.log', mode='a')
handler.setFormatter(formatter)
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.addHandler(screen_handler)


def _print_info(msg):
    logger.info("-"*150)
    logger.info("\t"+msg)
    logger.info("-"*150)


def raise_value_error(msg):
    msg = "*="*80 + "\n\t" + str(msg) + "\n" + "*="*80
    raise ValueError(msg)


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
    """Load csv using pyspark

    Args:
        data_path (str): path to csv file
        spark (SparkSession): spark session
        columns_mapping (dict): dictionary mapping old column names with new ones
        schema (StructType): type mapping for each column
        fill_na (dict): mapping with column name -> replacement value, for filling missing values

    Returns:
        DataFrame: loaded csv as dataframe
    """
    df = spark.read.csv(data_path, sep=";", header=True, schema=schema)
    df = df\
        .select([F.col(c).alias(columns_mapping.get(c, c)) for c in df.columns])\
        .fillna(fill_na)
    return df


def load_json_spark(data_path, spark, list_expr, fill_na):
    """load json file using spark

    Args:
        data_path (str): path to json file
        spark (SparkSession): spark session
        list_expr (list[str]): list of expressions extracting nested json fields to create dataframe columns
        fill_na (dict): mapping with column name -> replacement value, for filling missing values

    Returns:
        DataFrame: loaded json as dataframe
    """
    return spark.read.json(data_path)\
        .selectExpr(*list_expr)\
        .fillna(fill_na)


def check_unique_key(df, key, table_name):
    """Check that key column key is unique for table df named `table_name`
    """
    n_rows = df.count()
    n_unique_keys = df.select(key).distinct().count()
    if n_rows != n_unique_keys:
        raise_value_error(
            f"Check of unique key constraint failed for table {table_name}")
    _print_info(
        f"Check of unique key constraint succeeded for table {table_name}")


def check_non_empty(df, table_name):
    """Checks if dataframe df has at least 1 row
    """
    if df.count() < 1:
        raise_value_error(f"Non-empty check failed for table {table_name}")
    _print_info(f"Non-empty check succeeded for table {table_name}")


def check_min(df, column, min_val, table_name):
    """Checks if column of dataframe df has all values greater than min_val
    """
    table_min = df.select(F.min(column)).collect()[0][0]
    if table_min <= min_val:
        raise_value_error(
            f"Test failed! Minimum value ({table_min}) of column {column} of table {table_name} lower than threshold ({min_val})")
    _print_info(
        f"Minimum value check succeeded for column {column} of table {table_name}")


def check_range(df, min_col, max_col, table_name):
    """checks if values of column min_col are lower than values of column max_col (min and max temperature).
        If min_col > max_col for a given row, value of max_col is set equal to min_col for this row
    """
    count = df.filter(f"{min_col} > {max_col}").count()
    if count > 0:
        _print_info(
            f"Range check failed! In {count} rows of table {table_name}, {min_col} is higher than {max_col}.\n--> Fixing it ")
        return df.withColumn(max_col,
                             F.when(df[max_col] < df[min_col], df[min_col])
                             .otherwise(df[max_col]))
    _print_info(
        f"Range check succeeded for columns {min_col} and {max_col} of table {table_name}")
    return df


def process_fact_and_dim_tables(df_loss_declared, df_declared_and_found, df_stations, df_temperature, out_path):
    """Create all fact and dimension tables, apply some data quality checks and save data to parquet files

    Args:
        df_loss_declared (DataFrame): data frame for loss declaration
        df_declared_and_found (DataFrame): data frame for declared and found properties
        df_stations (DataFrame): data frame for stations
        df_temperature (DataFrame): data frame for temperatures
        out_path (str): output folder path
    """
    # create date columns from timestamp
    df_loss_declared = df_loss_declared.withColumn(
        "date", F.to_date(F.col("date_and_time")))
    df_declared_and_found = df_declared_and_found.withColumn(
        "date", F.to_date(F.col("date_and_time")))

    # check station nb of platforms quality, and do joins with stations
    check_min(df_stations.where("station_id != 'NOT_A_STATION'"),
              "station_number_of_platforms", 0, "stations")
    df_loss_declared = df_loss_declared\
        .join(df_stations.select("station_id", "station_county_code"), "station_id", "inner")
    df_declared_and_found = df_declared_and_found\
        .join(df_stations.select("station_id", "station_county_code"), "station_id", "inner")

    # temperature quality check and join
    df_temperature = check_range(df_temperature, 'min_temperature',
                                 'max_temperature', "temperature")
    df_loss_declared = df_loss_declared\
        .join(df_temperature, (df_loss_declared.station_county_code == df_temperature.county_code) & (df_loss_declared.date == df_temperature.date), "left")
    df_declared_and_found = df_declared_and_found\
        .join(df_temperature, (df_declared_and_found.station_county_code == df_temperature.county_code) & (df_declared_and_found.date == df_temperature.date), "left")

    # check found and recovery date ranges,
    df_declared_and_found = check_range(df_declared_and_found, 'date_and_time',
                                        'recovery_date', "declared_and_found_properties")
    # add delay in hours between declaration and recovery times
    diff_secs_col = F.col("recovery_date").cast(
        "long") - F.col("date_and_time").cast("long")
    df_declared_and_found = df_declared_and_found.withColumn(
        "delay_before_recovery", diff_secs_col/3600.0)
    
    # check non-empty and save loss declaration
    save_path = os.path.join(out_path, "loss_declaration")
    df_loss_declared = df_loss_declared\
        .withColumn("declaration_id", F.monotonically_increasing_id())\
        .select('declaration_id', 'date_and_time', 'station_id', 'property_nature', 'property_type',
                'min_temperature', 'max_temperature', 'avg_temperature')
    check_non_empty(df_loss_declared, "declared_loss")
    check_unique_key(df_loss_declared, "declaration_id", "loss_declaration")
    df_loss_declared\
        .write\
        .partitionBy('station_id', 'property_type')\
        .parquet(save_path, mode="overwrite")

    # check non-empty and save declared and found properties
    save_path = os.path.join(out_path, "declared_and_found")
    df_declared_and_found = df_declared_and_found\
        .withColumn("found_id", F.monotonically_increasing_id())\
        .select('found_id', 'date_and_time', 'station_id', 'property_nature', 'property_type', 'recovery_date',
                'delay_before_recovery', 'min_temperature', 'max_temperature', 'avg_temperature')
    check_non_empty(df_declared_and_found, "declared_and_found_properties")
    check_unique_key(df_declared_and_found, "found_id",
                     "declared_and_found_properties")
    df_declared_and_found\
        .write\
        .partitionBy('station_id', 'property_type')\
        .parquet(save_path, mode="overwrite")

    # check non-empty and save stations
    save_path = os.path.join(out_path, "stations")
    check_non_empty(df_stations, "stations")
    check_unique_key(df_stations, "station_id", "stations")
    df_stations\
        .write\
        .parquet(save_path, mode="overwrite")

    # check non-empty and save time
    save_path = os.path.join(out_path, "time")
    df_time = reduce(pyspark.sql.DataFrame.unionAll, [df_loss_declared.select("date_and_time"),
                                                      df_declared_and_found.select("date_and_time"), df_declared_and_found.select("recovery_date")])\
        .distinct()\
        .withColumn("hour", F.hour(F.col("date_and_time")))\
        .withColumn("day", F.dayofmonth(F.col("date_and_time")))\
        .withColumn("week", F.weekofyear(F.col("date_and_time")))\
        .withColumn("month", F.month(F.col("date_and_time")))\
        .withColumn("year", F.year(F.col("date_and_time")))\
        .withColumn("weekday", F.date_format(F.col("date_and_time"), "E"))
    check_non_empty(df_time, "time")
    check_unique_key(df_time, "date_and_time", "time")
    df_time\
        .write\
        .partitionBy("year", "month")\
        .parquet(save_path, mode="overwrite")


def main():
    spark = create_spark_session()

    # define path to datasets
    path_declared_loss = "input_data/objets-trouves-gares.csv"
    path_declared_and_found = "input_data/objets-trouves-restitution.csv"
    path_stations = "input_data/referentiel-gares-voyageurs.json"
    path_temperature = "input_data/temperature-quotidienne-departementale.csv"

    _print_info("Loading loss declarations data")
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

    _print_info("Loading lost and found properties data")
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

    _print_info("Loading temperatures data")
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

    _print_info("Loading stations data")
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

    _print_info("Processing and saving fact and dimension tables")
    out_path = "output_data"
    process_fact_and_dim_tables(
        df_loss_declared, df_declared_and_found, df_stations, df_temperature, out_path)
    _print_info("Processing ended | Data saved at `output_data` folder")

if __name__ == "__main__":
    main()
