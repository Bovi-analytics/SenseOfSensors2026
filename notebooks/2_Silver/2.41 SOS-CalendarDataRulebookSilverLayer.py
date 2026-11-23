# Databricks notebook source
from pyspark.sql import functions as F

CATALOG       = "senseofsensors_catalog"
SILVER_SCHEMA = f"{CATALOG}.silver"
DATASET       = "CalendarData"

print("Writing CalendarData rules to shared metadata tables")
print(f"Schema: {SILVER_SCHEMA}")

# COMMAND ----------

# One row per column in the silver CalendarData table
rulebook_rows = [
    # (Dataset, SourceColumn, TargetColumn, DataType, Nullable, Strategy)
    (DATASET, "AnimalIdentifier", "AnimalIdentifier", "string",    True,  "keep"),
    (DATASET, "HerdIdentifier",   "HerdIdentifier",   "string",    False, "keep"),
    (DATASET, "EventDate",        "EventDate",         "timestamp", True,  "parse_multi_format"),
    (DATASET, "ObservationType",  "ObservationType",   "string",    False, "keep"),
    (DATASET, "ObservationValue", "ObservationValue",  "string",    True,  "keep"),
    (DATASET, "AttributeMap",     "AttributeMap",      "map",       True,  "keep"),
    (DATASET, "DataBatch",        "DataBatch",         "string",    False, "derived"),
]

rulebook_df = spark.createDataFrame(rulebook_rows, [
    "Dataset", "SourceColumn", "TargetColumn", "DataType", "Nullable", "Strategy"
])

rulebook_df.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.Rulebook")

print(f"Written {rulebook_df.count()} rows to Rulebook")
display(rulebook_df)

# COMMAND ----------

# ObservationType English labels
code_rows = [
    (DATASET, "ObservationType", "CalvingEvent",         "CalvingEvent",         "Cow calved — sensor assigned to new lactation"),
    (DATASET, "ObservationType", "DryoffEvent",          "DryoffEvent",          "Cow dried off — sensor removed"),
    (DATASET, "ObservationType", "HeatEvent",            "HeatEvent",            "Heat detected by sensor"),
    (DATASET, "ObservationType", "InseminationEvent",    "InseminationEvent",    "Cow inseminated"),
    (DATASET, "ObservationType", "BirthEvent",           "BirthEvent",           "Calf born"),
    (DATASET, "ObservationType", "PregnancyCheckEvent",  "PregnancyCheckEvent",  "Pregnancy check performed"),
    (DATASET, "ObservationType", "KeepOpenEvent",        "KeepOpenEvent",        "Cow kept open — not bred"),
    (DATASET, "ObservationType", "CustomEvent",          "CustomEvent",          "Farm-defined custom event"),
]

codes_df = spark.createDataFrame(code_rows, [
    "Dataset", "ColumnName", "CodeValue", "MappedValue", "Description"
])

codes_df.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.CodeMappings")

print(f"Written {codes_df.count()} rows to CodeMappings")
display(codes_df)

# COMMAND ----------

print("Rulebook rows for CalendarData:")
display(spark.table(f"{SILVER_SCHEMA}.Rulebook")
    .where(F.col("Dataset") == DATASET))

print("\nCleaningConstraints rows for CalendarData:")
display(spark.table(f"{SILVER_SCHEMA}.CleaningConstraints")
    .where(F.col("Dataset") == DATASET))

print("\nCodeMappings rows for CalendarData:")
display(spark.table(f"{SILVER_SCHEMA}.CodeMappings")
    .where(F.col("Dataset") == DATASET))