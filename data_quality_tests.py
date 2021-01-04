from pyspark.sql import functions as F
from utils import _print_info, raise_value_error


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
