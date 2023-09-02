import findspark
from pyspark.sql import SparkSession

from extraction import extract_data_from_csv
from transformation import transformation
from load import load_data

findspark.init()
print(findspark.find())

file_path = r'data/insurance.csv'

spark = SparkSession.builder.master('local[1]').appName('SparkETLPipeline').getOrCreate()

data = extract_data_from_csv(spark, file_path, has_header=True)

data.show(5)

print('*' * 65)

data, indexers = transformation(data)
data.show(5)

# to write to data dictionary
print({indexer.getInputCol(): indexer.labels for indexer in indexers})

save_path = r'data/output.csv'
# load_data(data, save_path)

print("*" * 75)
data_new = extract_data_from_csv(spark, save_path, has_header=True)
data_new.show(5)
