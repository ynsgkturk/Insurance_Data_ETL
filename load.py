from pyspark.sql import DataFrame


def load_data(data_df: DataFrame, output_csv_path: str, trino_connection_url: str = None, trino_table_name: str = None,
              user: str = None, password: str = None):
    """
    Loads data into CSV files and optionally into a Trino table.

    :param data_df: The input PySpark DataFrame to be loaded.
    :param output_csv_path: The path to save CSV files.
    :param trino_connection_url: Trino connection URL (e.g., "trino://localhost:8080").
    :param trino_table_name: Name of the Trino table to create.
    :param user: Username credential to connect.
    :param password: Password credential to connect.
    :return: None
    """
    # Save data as CSV files
    data_df.toPandas().to_csv(output_csv_path, index=False, header=True)

    # Optionally, load data into Trino table
    if trino_connection_url and trino_table_name:
        data_df.write \
            .format("jdbc") \
            .option("url", trino_connection_url) \
            .option('user', user) \
            .option('password', password) \
            .option('driver', "trino_driver") \
            .option("dbtable", trino_table_name) \
            .save()







