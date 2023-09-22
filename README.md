# Overview
This project is an example of an ETL (Extract, Transform, Load) pipeline for [insurance data](https://www.kaggle.com/datasets/mirichoi0218/insurance).
It demonstrates how to extract data from a CSV file, perform data transformations using PySpark, and load the transformed data into CSV files or a Trino (Presto) database.

# ETL Steps
- Extract the data<br/>
In this step we extract the data from csv file and trino (not tested yet.)

`file_path = "data/insurance_dataset.csv" `<br>
`extracted_data = extract_data_from_csv(spark, file_path)`

- Transform Data <br/>
In this step we do handle missing/duplicate variables, create futures, and convert usable format.

``data, indexers = transformation(data)``

- Load Data<br/>
Transformed data load into trino (not tested yet) or csv.

``load_data(data, save_path)``

# Kaggle Notebook
Check [this kaggle notebook]() that I prepared for Exploratory Data Analysis (EDA) and modeling. 
