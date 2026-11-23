# Databricks notebook source
#SenseOfSensors Bronze-Silver-Gold Pipeline
#Bronze Notebook

#This notebook creates a Unity Catalog BSG tables from SenseOfSensors dataset stored in blob_senseofsensors volume. This is the first notebook in the pipeline 

# COMMAND ----------

# DBTITLE 1,Cell 2

display(dbutils.fs.ls("/Volumes/senseofsensors_catalog/senseofsensors_schema/blob_senseofsensors/ParquetData/"))

# COMMAND ----------

#import
from pyspark.sql import functions as F

# COMMAND ----------

#Configuration

#Unity Catalog
CATALOG = "senseofsensors_catalog"
SCHEMA = "senseofsensors_schema"

#BSG Layers
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA ="gold"

#Volume
VOLUME = "/Volumes/senseofsensors_catalog/senseofsensors_schema/blob_senseofsensors"

#main dataset present in:
PARQUET_FOLDER =f"{VOLUME}/ParquetData"
CSV_FOLDER = "f{VOLUME}/CsvData"
#more folders here post analysis?

print("Volume path:", VOLUME)
print("Parquet folder:", PARQUET_FOLDER)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA senseofsensors_catalog.bronze;
# MAGIC CREATE SCHEMA senseofsensors_catalog.silver;
# MAGIC CREATE SCHEMA senseofsensors_catalog.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC Move Raw Dataset to Bronze Schema
# MAGIC
# MAGIC Datasets include:
# MAGIC 1. Sensor Data
# MAGIC 2. Reference Data
# MAGIC 3. TemperatureHumidityIndex Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS senseofsensors_catalog.bronze.bronze_volume;

# COMMAND ----------

# SensorDataRepartitioned22032021 -> SensorData
dbutils.fs.cp(
    "/Volumes/senseofsensors_catalog/senseofsensors_schema/blob_senseofsensors/ParquetData/SensorDataRepartitioned22032021/",
    "/Volumes/senseofsensors_catalog/bronze/bronze_volume/SensorData/",
    recurse=True
)

# ReferenceData -> ReferenceData
dbutils.fs.cp(
    "/Volumes/senseofsensors_catalog/senseofsensors_schema/blob_senseofsensors/ParquetData/ReferenceData/",
    "/Volumes/senseofsensors_catalog/bronze/bronze_volume/ReferenceData/",
    recurse=True
)

# TemperatureHumidityIndex -> TemperatureHumidityIndex
dbutils.fs.cp(
    "/Volumes/senseofsensors_catalog/senseofsensors_schema/blob_senseofsensors/Data06032019/TemperatureHumidityIndex/",
    "/Volumes/senseofsensors_catalog/bronze/bronze_volume/TemperatureHumidityIndex/",
    recurse=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we move to Silver Layer.