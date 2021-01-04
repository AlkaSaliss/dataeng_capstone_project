import os
from functools import reduce

import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, FloatType, StringType, StructType,
                               TimestampType)

from utils import create_spark_session, _print_info, load_csv_spark, load_json_spark
from data_quality_tests import check_min, check_non_empty, check_range, check_unique_key


def process_stations(df_stations, out_path):
    """Create `stations` dimension table, and apply non empty, unique primary key constraint and values range checks

    Args:
        df_stations (DataFrame): data frame for stations
        out_path (str): output folder path where to save the parquet files
    """
    _print_info("Processing stations dimension table")
    # check minimum number of platform for each station, check non empty table and check unique key constraint
    check_min(df_stations.where("station_id != 'NOT_A_STATION'"),
              "station_number_of_platforms", 0, "stations")
    check_non_empty(df_stations, "stations")
    check_unique_key(df_stations, "station_id", "stations")
    
     # save table
    save_path = os.path.join(out_path, "stations")
    df_stations\
        .write\
        .parquet(save_path, mode="overwrite")



def process_time(df_loss_declared, df_declared_and_found, out_path):
    """Create `time` dimension table, and apply non empty, unique primary key constraint checks

    Args:
        df_loss_declared (DataFrame): data frame for loss declaration
        df_declared_and_found (DataFrame): data frame for declared and found properties
        out_path (str): output folder path where to save the parquet files
    """
    _print_info("Processing time dimension table")
    # create different time related columns
    df_time = reduce(pyspark.sql.DataFrame.unionAll, [df_loss_declared.select("date_and_time"),
                                                      df_declared_and_found.select("date_and_time"), df_declared_and_found.select("recovery_date")])\
        .distinct()\
        .withColumn("hour", F.hour(F.col("date_and_time")))\
        .withColumn("day", F.dayofmonth(F.col("date_and_time")))\
        .withColumn("week", F.weekofyear(F.col("date_and_time")))\
        .withColumn("month", F.month(F.col("date_and_time")))\
        .withColumn("year", F.year(F.col("date_and_time")))\
        .withColumn("weekday", F.date_format(F.col("date_and_time"), "E"))
    
    # check non-empty and unique key constraint
    check_non_empty(df_time, "time")
    check_unique_key(df_time, "date_and_time", "time")

    # save table
    save_path = os.path.join(out_path, "time")
    df_time\
        .write\
        .partitionBy("year", "month")\
        .parquet(save_path, mode="overwrite")
    

def process_loss_declaration(df_loss_declared, df_stations, df_temperature, out_path):
    """Create `loss_declaration` fact table, and apply the non-empty and unique key constraint checks

    Args:
        df_loss_declared (DataFrame): data frame for loss declaration
        df_stations (DataFrame): data frame for stations
        df_temperature (DataFrame): data frame for temperatures
        out_path (str): output folder path where to save the parquet files
    """
    _print_info("Processing loss_declaration fact table")
    # joins with stations table
    df_loss_declared = df_loss_declared\
        .join(df_stations.select("station_id", "station_county_code"), "station_id", "inner")
    
    # create date columns from timestamp and join with temperature table
    df_loss_declared = df_loss_declared.withColumn(
        "date", F.to_date(F.col("date_and_time")))
    df_loss_declared = df_loss_declared\
        .join(df_temperature, (df_loss_declared.station_county_code == df_temperature.county_code) & (df_loss_declared.date == df_temperature.date), "left")
    
    # add sequential primary key
    df_loss_declared = df_loss_declared\
        .withColumn("declaration_id", F.monotonically_increasing_id())\
        .select('declaration_id', 'date_and_time', 'station_id', 'property_nature', 'property_type',
                'min_temperature', 'max_temperature', 'avg_temperature')
    
     # check non-empty and unique primary constraint 
    check_non_empty(df_loss_declared, "declared_loss")
    check_unique_key(df_loss_declared, "declaration_id", "loss_declaration")
    
    # save table
    save_path = os.path.join(out_path, "loss_declaration")
    df_loss_declared\
        .write\
        .partitionBy('station_id', 'property_type')\
        .parquet(save_path, mode="overwrite")


def process_declared_and_found(df_declared_and_found, df_stations, df_temperature, out_path):
    """Create `declared_and_found` fact table, and apply range quality check for recovery date,  the non-empty and unique key constraint checks

    Args:
        df_declared_and_found (DataFrame): data frame for lost and found properties
        df_stations (DataFrame): data frame for stations
        df_temperature (DataFrame): data frame for temperatures
        out_path (str): output folder path where to save the parquet files
    """
    _print_info("Processing declared_and_found fact table")
    # do joins with stations
    df_declared_and_found = df_declared_and_found\
        .join(df_stations.select("station_id", "station_county_code"), "station_id", "inner")

    # create date columns from timestamp and join with temperature table
    df_declared_and_found = df_declared_and_found.withColumn(
        "date", F.to_date(F.col("date_and_time")))
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
    
    # add sequential primary key 
    df_declared_and_found = df_declared_and_found\
        .withColumn("found_id", F.monotonically_increasing_id())\
        .select('found_id', 'date_and_time', 'station_id', 'property_nature', 'property_type', 'recovery_date',
                'delay_before_recovery', 'min_temperature', 'max_temperature', 'avg_temperature')
    
     # check non-empty and unique primary key constraint
    check_non_empty(df_declared_and_found, "declared_and_found_properties")
    check_unique_key(df_declared_and_found, "found_id",
                     "declared_and_found_properties")
    
    # save table
    save_path = os.path.join(out_path, "declared_and_found")
    df_declared_and_found\
        .write\
        .partitionBy('station_id', 'property_type')\
        .parquet(save_path, mode="overwrite")

def main():
    # get spark session
    spark = create_spark_session("LOPRO2TS")

    #############################################################################################################################
    # 1. define path to datasets
    #############################################################################################################################
    path_declared_loss = "input_data/objets-trouves-gares.csv"
    path_declared_and_found = "input_data/objets-trouves-restitution.csv"
    path_stations = "input_data/referentiel-gares-voyageurs.json"
    path_temperature = "input_data/temperature-quotidienne-departementale.csv"

    #############################################################################################################################
    # 2. Read loss declaration dataset
    #############################################################################################################################
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
    
    #############################################################################################################################
    # 3. Read lost and found properties dataset
    #############################################################################################################################
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

    #############################################################################################################################
    # 4. Read temperature dataset
    #############################################################################################################################
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
     # check temperature quality between min and max (i.e. min <= max)
    df_temperature = check_range(df_temperature, 'min_temperature',
                                 'max_temperature', "temperature")
    
    #############################################################################################################################
    # 5. Read stations dataset
    #############################################################################################################################
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
    #############################################################################################################################
    # 6. Process stations dimension table
    #############################################################################################################################
    process_stations(df_stations, out_path)

    #############################################################################################################################
    # 7. Process time dimension table
    #############################################################################################################################
    process_time(df_loss_declared, df_declared_and_found, out_path)

    #############################################################################################################################
    # 8. Process loss_declaration fact table
    #############################################################################################################################
    process_loss_declaration(df_loss_declared, df_stations, df_temperature, out_path)

    #############################################################################################################################
    # 9. Process loss_declaration fact table
    #############################################################################################################################
    process_declared_and_found(df_declared_and_found, df_stations, df_temperature, out_path)

    _print_info("Processing ended | Data saved at `output_data` folder")


if __name__ == "__main__":
    main()
