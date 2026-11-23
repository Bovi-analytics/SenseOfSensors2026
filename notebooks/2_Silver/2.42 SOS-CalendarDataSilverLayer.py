# Databricks notebook source
from pyspark.sql import functions as F

CATALOG        = "senseofsensors_catalog"
SILVER_SCHEMA  = f"{CATALOG}.silver"
BRONZE_PATH    = "/Volumes/senseofsensors_catalog/bronze/bronze_volume/CalendarData/"
OUT_TABLE      = f"{SILVER_SCHEMA}.CalendarData"
DATASET        = "CalendarData"

# Farm IDs are taken from the bronze file names (<farm>_calendarData_<batch>.csv), so no real farm ID is
# written in this notebook; they are replaced by Farm_A … Farm_H later, by anonymise().
FARMS = sorted({f.name.split("_calendarData_")[0] for f in dbutils.fs.ls(BRONZE_PATH) if "_calendarData_" in f.name})

# Date formats from original fromMultipleDateFormatsUDF
DATE_FORMATS = ["MM/dd/yyyy", "dd/MM/yyyy", "dd-MM-yy", "yyyy-MM-dd HH:mm", "yyyy-MM-dd"]

BLANK_TOKENS = ["NA", "N/A", "", "null", "NULL", "-"]

print(f"Reading from : {BRONZE_PATH}")
print(f"Writing to   : {OUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Private step.** The next cell loads the project's anonymisation helper, which is kept in the private
# MAGIC Databricks workspace and is not part of this repository. It defines `anonymise(df, dataset)`, which replaces
# MAGIC farm IDs with `Farm_A`–`Farm_H`, pseudonymises animal and life-number IDs with a secret key, and drops
# MAGIC free-text fields that can contain identifiers. Everything from silver onwards is therefore anonymous.

# COMMAND ----------

# MAGIC %run "./0b SOS-AnonymisationHelper"

# COMMAND ----------

rulebook_rows = [
    # (Dataset, SourceColumn, TargetColumn, DataType, Nullable, Strategy, Ordinal)
    (DATASET, "AnimalIdentifier", "AnimalIdentifier", "string",    True,  "keep",                1),
    (DATASET, "HerdIdentifier",   "HerdIdentifier",   "string",    False, "keep",                2),
    (DATASET, "EventDate",        "EventDate",        "timestamp", True,  "parse_multi_format",  3),
    (DATASET, "ObservationType",  "ObservationType",  "string",    False, "keep",                4),
    (DATASET, "ObservationValue", "ObservationValue", "string",    True,  "keep",                5),
    (DATASET, "AttributeMap",     "AttributeMap",     "map",       True,  "keep",                6),
    (DATASET, "DataBatch",        "DataBatch",        "string",    False, "derived",             7),
]

rulebook_df = spark.createDataFrame(rulebook_rows, [
    "Dataset", "SourceColumn", "TargetColumn", "DataType", "Nullable", "Strategy", "Ordinal"
])

# COMMAND ----------

# Translated directly from the original Scala:
# calendarTransformationMap = Map("Cow number" -> "AnimalIdentifier",
#                                 "Datetime"   -> "EventDate",
#                                 "Type"       -> "ObservationType")
# rawData1.union(rawData2).distinct()
# ObservationValue = null (calendar events carry no numeric value)

from functools import reduce

def read_farm_batch(farm, batch):
    path = f"{BRONZE_PATH}{farm}_calendarData_{batch}.csv"
    try:
        df = (spark.read
            .option("header", "true")
            .option("delimiter", ",")
            .option("nullValue", "NA")
            .csv(path)
            .withColumnRenamed("Cow number", "AnimalIdentifier")
            .withColumnRenamed("Datetime",   "EventDate")
            .withColumnRenamed("Type",       "ObservationType")
            .withColumn("HerdIdentifier",   F.lit(farm))
            .withColumn("DataBatch",        F.lit(batch))
            .withColumn("ObservationValue", F.lit(None).cast("string"))
        )
        # Keep remaining columns as AttributeMap
        keep_cols = {"AnimalIdentifier", "EventDate", "ObservationType",
                     "HerdIdentifier", "DataBatch", "ObservationValue"}
        attr_cols = [c for c in df.columns if c not in keep_cols]
        if attr_cols:
            df = df.withColumn("AttributeMap",
                F.map_from_arrays(
                    F.array(*[F.lit(c) for c in attr_cols]),
                    F.array(*[F.col(c).cast("string") for c in attr_cols])
                )
            ).drop(*attr_cols)
        else:
            df = df.withColumn("AttributeMap",
                F.lit(None).cast("map<string,string>"))
        return df
    except Exception as e:
        print(f"   {farm} {batch}: {e}")
        return None

dfs = []
for farm in FARMS:
    for batch in ["batch1", "batch2"]:
        df = read_farm_batch(farm, batch)
        if df is not None:
            dfs.append(df)
            print(f" {farm} {batch}: {df.count():,} rows")

raw = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
print(f"\nTotal raw rows (both batches, all farms): {raw.count():,}")

# COMMAND ----------

DATE_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "dd-MM-yy",
]

# Step 1 — Parse EventDate, handling timezone offset format "2009-06-28 00:00:00 +0200"
parsed = raw.withColumn("EventDate",
    F.coalesce(
        F.to_timestamp(
            F.regexp_replace(F.col("EventDate"), r"\s[+-]\d{4}$", ""),
            "yyyy-MM-dd HH:mm:ss"
        ),
        *[F.to_timestamp(F.col("EventDate"), fmt) for fmt in DATE_FORMATS]
    )
)

# Step 2 — Drop corrupt dates (year < 2000) and null dates
# A cow cannot have calved, dried off or been born before year 2000
before = parsed.count()
parsed = parsed.where(
    F.col("EventDate").isNotNull() &
    (F.year(F.col("EventDate")) >= 2000)
)
after = parsed.count()

print(f"Parsed successfully : {after:,}")
print(f"Dropped (null date) : {before - after:,}")

# Show what was dropped
display(raw.where(
    F.coalesce(
        F.to_timestamp(
            F.regexp_replace(F.col("EventDate"), r"\s[+-]\d{4}$", ""),
            "yyyy-MM-dd HH:mm:ss"
        ),
        *[F.to_timestamp(F.col("EventDate"), fmt) for fmt in DATE_FORMATS]
    ).isNull() |
    (F.year(F.coalesce(
        F.to_timestamp(
            F.regexp_replace(F.col("EventDate"), r"\s[+-]\d{4}$", ""),
            "yyyy-MM-dd HH:mm:ss"
        ),
        *[F.to_timestamp(F.col("EventDate"), fmt) for fmt in DATE_FORMATS]
    )) < 2000)
).select("HerdIdentifier", "ObservationType", "EventDate").distinct().limit(20))

# COMMAND ----------

# Replace blank tokens with null
for col_name in ["AnimalIdentifier", "ObservationType"]:
    parsed = parsed.withColumn(col_name,
        F.when(F.col(col_name).isin(BLANK_TOKENS), None)
         .otherwise(F.col(col_name)))

# Date range filter — flag out of range, keep rows
MIN_DATE = "2016-01-01"
MAX_DATE = "2021-12-31"

calendar = (parsed
    .withColumn("Warnings", F.array())
    .withColumn("Warnings",
        F.when(
            F.col("EventDate").isNull(),
            F.array_append(F.col("Warnings"), F.lit("InvalidEventDate:EventDate (set to null)"))
        ).otherwise(F.col("Warnings"))
    )
    .withColumn("Warnings",
        F.when(
            F.col("EventDate").isNotNull() & (
                (F.col("EventDate") < F.lit(MIN_DATE).cast("timestamp")) |
                (F.col("EventDate") > F.lit(MAX_DATE).cast("timestamp"))
            ),
            F.array_append(F.col("Warnings"), F.lit("OutOfExpectedRange:EventDate (kept)"))
        ).otherwise(F.col("Warnings"))
    )
    .withColumn("Warnings",
        F.when(
            F.col("ObservationType").isNull(),
            F.array_append(F.col("Warnings"), F.lit("MissingRequired:ObservationType"))
        ).otherwise(F.col("Warnings"))
    )
    .withColumn("UnknownObservationType",
        F.col("ObservationType").isNotNull() & ~F.col("ObservationType").isin([
            "CalvingEvent", "DryoffEvent", "HeatEvent", "InseminationEvent",
            "BirthEvent", "PregnancyCheckEvent", "KeepOpenEvent", "CustomEvent"
        ])
    )
    .withColumn("Warnings",
        F.when(
            F.col("UnknownObservationType"),
            F.array_append(F.col("Warnings"), F.lit("UnknownObservationType:ObservationType (kept)"))
        ).otherwise(F.col("Warnings"))
    )
    .drop("UnknownObservationType")
    .withColumn("WarningCount", F.size(F.col("Warnings")))
)

print(f"Rows after cleaning: {calendar.count():,}")
print(f"Rows with warnings : {calendar.where(F.col('WarningCount') > 0).count():,}")

# COMMAND ----------

# Dedup key: HerdIdentifier + AnimalIdentifier + EventDate + ObservationType
# Both batches may overlap — distinct() removes exact duplicates across batches

DEDUP_COLS = ["HerdIdentifier", "AnimalIdentifier", "EventDate", "ObservationType"]

before = calendar.count()
calendar = calendar.dropDuplicates(DEDUP_COLS)
after  = calendar.count()

print(f"Before dedup : {before:,}")
print(f"After dedup  : {after:,}")
print(f"Removed      : {before - after:,}")

# COMMAND ----------

print("Event counts by type and farm:")
display(calendar
    .groupBy("HerdIdentifier", "ObservationType")
    .count()
    .orderBy("HerdIdentifier", "ObservationType"))

print("\nDate range per farm:")
display(calendar
    .groupBy("HerdIdentifier")
    .agg(
        F.min("EventDate").alias("EarliestEvent"),
        F.max("EventDate").alias("LatestEvent"),
        F.count("*").alias("TotalEvents")
    )
    .orderBy("HerdIdentifier"))

print("\nWarning summary:")
display(calendar
    .where(F.col("WarningCount") > 0)
    .select(F.explode("Warnings").alias("Warning"))
    .groupBy("Warning").count()
    .orderBy("count", ascending=False))

print("\nSample rows:")
display(calendar
    .where(F.col("ObservationType") == "DryoffEvent")
    .orderBy("HerdIdentifier", "AnimalIdentifier", "EventDate")
    .limit(20))

# COMMAND ----------

calendar = anonymise(calendar, "CalendarData")
(calendar
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("HerdIdentifier")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")

# COMMAND ----------

cal = spark.table(OUT_TABLE)

print("=" * 55)
print("  Silver — CalendarData Summary")
print("=" * 55)
print(f"  Total events     : {cal.count():>10,}")
print(f"  Distinct animals : {cal.select('AnimalIdentifier').distinct().count():>10,}")
print(f"  Distinct farms   : {cal.select('HerdIdentifier').distinct().count():>10,}")
print()
print("  Event type breakdown:")
display(cal.groupBy("ObservationType").count().orderBy("count", ascending=False))

# COMMAND ----------

display(spark.table("senseofsensors_catalog.silver.CalendarData")
    .groupBy("HerdIdentifier", "ObservationType")
    .agg(
        F.min("EventDate").alias("Earliest"),
        F.max("EventDate").alias("Latest"),
        F.count("*").alias("Events")
    )
    .orderBy("HerdIdentifier", "ObservationType"))