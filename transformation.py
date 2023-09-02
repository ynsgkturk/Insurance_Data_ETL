from pyspark.ml.feature import StringIndexer, StringIndexerModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import when,col
from pyspark.sql.types import IntegerType


def transformation(dataset: DataFrame) -> tuple[DataFrame, list[StringIndexerModel]]:
    # Check missing values
    if dataset.toPandas().isna().any().any():
        dataset = dataset.dropna()

    # Check if there is any duplicates
    total_rows = dataset.count()
    distinct_rows = dataset.distinct().count()
    duplicate_rows = total_rows - distinct_rows

    print(f"There is %.d duplicate rows." % duplicate_rows)
    dataset = dataset.dropDuplicates()

    # Feature Engineering: Create BMI category based on BMI values
    dataset = dataset.withColumn("bmi_category",
                                 when((col("bmi") < 18.5), "Underweight")
                                 .when((col("bmi") >= 18.5) & (col("bmi") < 24.9), "Normal")
                                 .when((col("bmi") >= 25.0) & (col("bmi") < 29.9), "Overweight")
                                 .otherwise("Obese"))

    # Convert categorical columns
    string_columns = ["sex", "smoker", "region", "bmi_category"]
    indexers = [StringIndexer(inputCol=column, outputCol=f"{column}_index").fit(dataset) for column in string_columns]
    for indexer in indexers:
        dataset = indexer.transform(dataset)

    # Encode labels
    # encoder = OneHotEncoder(inputCols=[f"{col}_index" for col in string_columns],
    #                         outputCols=[f"{col}_encoded" for col in string_columns])
    # dataset = encoder.fit(dataset).transform(dataset)

    # Cast indexed columns from DoubleType to IntegerType
    columns_to_cast = ["sex_index", "smoker_index", "region_index", "bmi_category_index"]

    for col_name in columns_to_cast:
        dataset = dataset.withColumn(col_name, col(col_name).cast(IntegerType()))

    # Drop unnecessary columns
    columns_to_drop = ["sex", "smoker", "region", "bmi_category"]
    dataset = dataset.drop(*columns_to_drop)

    # rename columns
    for col_name in columns_to_cast:
        dataset = dataset.withColumnRenamed(col_name, col_name.replace("_index", ""))

    return dataset, indexers
