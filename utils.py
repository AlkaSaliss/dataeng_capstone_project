import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def set_logger():
    """helper function to set up correclty logging formatting
    """
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
    return logger


LOGGER = set_logger()


def _print_info(message):
    """Helper function to print pretty message"""
    LOGGER.info("-"*150)
    LOGGER.info("\t"+message)
    LOGGER.info("-"*150)


def raise_value_error(message):
    """Helper function to raise ValueError exception with formatted message"""
    message = "*="*80 + "\n\t" + str(message) + "\n" + "*="*80
    raise ValueError(message)


def create_spark_session(name):
    """Creates a spark session if it doesn't exist and returns it

    Returns:
        SparkSession: spark session
    """
    spark = SparkSession \
        .builder \
        .appName(name) \
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
