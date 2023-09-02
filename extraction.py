from pyspark.sql import SparkSession, DataFrame, types as T


def extract_data_from_csv(spark: SparkSession, file_path: str,
                          has_header: bool = True, schema: T.StructType = None) -> DataFrame:
    """
    Extracts data from a CSV file and returns a PySpark DataFrame

    :param spark: PySpark Session instance
    :param file_path: The path to the CSV file.
    :param has_header: Whether the data file has a header row (default: True)
    :param schema: Optional custom schema for the data (default: None)
    :return: A PySpark DataFrame containing the extracted data.
    """

    if schema:
        data = spark.read.csv(file_path, header=has_header, schema=schema)
    else:
        data = spark.read.csv(file_path, header=has_header, inferSchema=True)

    return data


def extract_data_from_trino(spark: SparkSession, trino_connection_url: str, user: str,
                            password: str, trino_table_name: str) -> DataFrame:
    """
    Extracts data from a Trino table and returns a PySpark DataFrame.

    :param spark: PySpark Session instance
    :param trino_connection_url: Trino connection URL (e.g., "trino://localhost:8080").
    :param user: Username credential
    :param password: Password credential
    :param trino_table_name: Name of the Trino table.

    :return: A PySpark DataFrame containing the extracted data.
    """
    # Read data from Trino table into a PySpark DataFrame
    data_df = spark.read \
        .format("jdbc") \
        .option("url", trino_connection_url) \
        .option('user', user) \
        .option('password', password) \
        .option('driver', "trino_driver") \
        .option("dbtable", trino_table_name) \
        .load()

    return data_df
