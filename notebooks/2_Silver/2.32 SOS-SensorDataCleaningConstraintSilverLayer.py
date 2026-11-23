# Databricks notebook source
# Databricks notebook source
# SOS SensorData — Silver Layer 1 Cleaning
# Reads all 23 sub-folders from `bronze_volume/SensorData/`, unions them, cleans, and writes one
# Delta table to `silver_volume/SensorData` partitioned by `ObservationType`.
# This is Silver Layer 1 — cleaned and standardised, `AttributeMap` kept as a map column.
# Silver Layer 2 (future) will crack open `AttributeMap` into named columns.

# 1.2 billion rows
# - No `coalesce(1)` — Spark decides file count per partition
# - Partitioned by `ObservationType` (23 partitions) so queries filter efficiently
# - No Raw backup columns
# - Processed folder-by-folder to avoid loading everything into memory at once
# Steps: 1 Read + union bronze · 2 Load rules · 3 Trim/blank · 4 Identity cleaning ·
# 5 Validate timestamps · 6 Cast ObservationValue · 7 Normalise AttributeMap keys ·
# 8 Required checks · 9 Dedup · 10 Combine warnings · 11 Audit · 12 Write · 13 Summary

# COMMAND ----------

# MAGIC %md
# MAGIC **Private step.** The next cell loads the project's anonymisation helper, which is kept in the private
# MAGIC Databricks workspace and is not part of this repository. It defines `anonymise(df, dataset)`, which replaces
# MAGIC farm IDs with `Farm_A`–`Farm_H`, pseudonymises animal and life-number IDs with a secret key, and drops
# MAGIC free-text fields that can contain identifiers. Everything from silver onwards is therefore anonymous.

# COMMAND ----------

# MAGIC %run "./0b SOS-AnonymisationHelper"

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("bronze_volume", "/Volumes/senseofsensors_catalog/bronze/bronze_volume")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG        = dbutils.widgets.get("catalog")
BRONZE_VOLUME  = dbutils.widgets.get("bronze_volume").rstrip("/")
SILVER_VOLUME  = dbutils.widgets.get("silver_volume").rstrip("/")
SILVER_SCHEMA  = f"{CATALOG}.silver"
DATASET        = "SensorData"
SRC_PATH       = f"{BRONZE_VOLUME}/{DATASET}/"
DATA_PATH      = f"{SILVER_VOLUME}/{DATASET}"
TARGET_TABLE   = f"{SILVER_SCHEMA}.{DATASET}"

_warn_exprs = []
def add_warn(expr): _warn_exprs.append(expr)
def pp(s): return dict(kv.split("=",1) for kv in (s or "").split(";") if "=" in kv)

# COMMAND ----------

# Step 1 — Read and union all 23 bronze sub-folders

folders = [f.path for f in dbutils.fs.ls(SRC_PATH)]
print(f"Found {len(folders)} sensor data sub-folders")

bronze = None
for path in sorted(folders):
    part = spark.read.parquet(path)
    bronze = part if bronze is None else bronze.unionByName(part)

BRONZE_ROWS = bronze.count()
print(f"Total bronze rows: {BRONZE_ROWS:,}  columns: {len(bronze.columns)}")
print(f"ObservationTypes: {bronze.select('ObservationType').distinct().count()}")

# COMMAND ----------

# Step 2 — Load rule tables

rules       = (spark.table(f"{SILVER_SCHEMA}.Rulebook")
               .where(F.col("Dataset") == DATASET).orderBy("Ordinal").collect())
constraints = [r.asDict() for r in spark.table(f"{SILVER_SCHEMA}.CleaningConstraints")
               .where(F.col("Dataset") == DATASET).collect()]
if not rules:
    raise ValueError("No Rulebook rows for SensorData — run SOS_SensorData_Rulebook first.")

rename_map    = {r["sourceColumn"]: r["targetColumn"] for r in rules}
target_order  = [r["targetColumn"] for r in rules]
type_map      = {r["targetColumn"]: r["dataType"] for r in rules}
required_cols = [r["targetColumn"] for r in rules if r["isRequired"]]
by_type = {}
for c in constraints:
    by_type.setdefault(c["constraintType"],[]).append(c)
print(f"{len(rename_map)} cols | {len(constraints)} constraints")

# COMMAND ----------

# Step 3 — Rename + trim + blankToNull

df = bronze
for src, tgt in rename_map.items():
    if src in df.columns:
        df = df.withColumnRenamed(src, tgt)
df = df.select([c for c in target_order if c in df.columns])

BLANK_TOKENS = ["", "null", "NULL", "Null", "NA", "N/A", "NaN", "nan", "-", ".", "?", " "]
for c, dt in df.dtypes:
    if dt == "string":
        t = F.trim(F.col(c))
        df = df.withColumn(c, F.when(t.isin(BLANK_TOKENS), None).otherwise(t))
print(f"columns: {df.columns}")

# COMMAND ----------

# Step 3b — Normalise ObservationType values to PascalCase
# The source values are camelCase with mixed underscore suffixes (e.g. `eatingTimeData15min`,
# `lyingBoutSummary_Max_boutlength`). Normalising to PascalCase makes the partition folder names
# consistent and readable (`EatingTimeData15min`, `LyingBoutSummary_Max_Boutlength`).
 
# COMMAND
 
OBS_TYPE_MAP = {
    "eatingBoutsTimeDataDay_avgBoutLength":          "EatingBoutsTimeDataDayAvgBoutLength",
    "eatingBoutsTimeDataDay_avgInterboutLength":      "EatingBoutsTimeDataDayAvgInterBoutLength",
    "eatingBoutsTimeDataDay_numberOfBouts":           "EatingBoutsTimeDataDayNumberOfBouts",
    "eatingTimeData15min":                            "EatingTimeData15min",
    "eatingTimeData2hr":                              "EatingTimeData2hr",
    "eatingTimeDataDay":                              "EatingTimeDataDay",
    "inactiveBoutsTimeDataDay_avgBoutLength":         "InactiveBoutsTimeDataDayAvgBoutLength",
    "inactiveBoutsTimeDataDay_avgInterboutLength":    "InactiveBoutsTimeDataDayAvgInterBoutLength",
    "inactiveBoutsTimeDataDay_numberOfBouts":         "InactiveBoutsTimeDataDayNumberOfBouts",
    "inactiveTimeData15min":                          "InactiveTimeData15min",
    "inactiveTimeData2hr":                            "InactiveTimeData2hr",
    "inactiveTimeDataDay":                            "InactiveTimeDataDay",
    "legActivityData":                                "LegActivityData",
    "legActivitySummary":                             "LegActivitySummary",
    "lyingBoutData":                                  "LyingBoutData",
    "lyingBoutSummary_Max_boutlength":                "LyingBoutSummaryMaxBoutLength",
    "lyingBoutSummary_Mean_boutlength":               "LyingBoutSummaryMeanBoutLength",
    "lyingBoutSummary_Median_boutlength":             "LyingBoutSummaryMedianBoutLength",
    "lyingBoutSummary_Min_boutlength":                "LyingBoutSummaryMinBoutLength",
    "lyingBoutSummary_Nr_of_bouts":                   "LyingBoutSummaryNrOfBouts",
    "lyingBoutSummary_lower_quartile_boutlength":     "LyingBoutSummaryLowerQuartileBoutLength",
    "lyingBoutSummary_upper_quartile_boutlength":     "LyingBoutSummaryUpperQuartileBoutLength",
    "lyingSummary":                                   "LyingSummary",
    "lyingTimeData":                                  "LyingTimeData",
    "ruminatingBoutsTimeDataDay_avgBoutLength":       "RuminatingBoutsTimeDataDayAvgBoutLength",
    "ruminatingBoutsTimeDataDay_avgInterboutLength":  "RuminatingBoutsTimeDataDayAvgInterboutLength",
    "ruminatingBoutsTimeDataDay_numberOfBouts":       "RuminatingBoutsTimeDataDayNumberOfBouts",
    "ruminatingTimeData15min":                        "RuminatingTimeData15min",
    "ruminatingTimeData2hr":                          "RuminatingTimeData2hr",
    "ruminatingTimeDataDay":                          "RuminatingTimeDataDay",
    "standingSummary":                                "StandingSummary",
    "standingTimeData":                               "StandingTimeData",
    "standupSummary":                                 "StandupSummary",
    "walkingSummary":                                 "WalkingSummary",
    "walkingTimeData":                                "WalkingTimeData",
}
 
# Build a single coalesce expression — unknown values kept as-is with a warning
mapping_expr = F.col("ObservationType")
for old, new in OBS_TYPE_MAP.items():
    mapping_expr = F.when(F.col("ObservationType") == old, F.lit(new)).otherwise(mapping_expr)
 
unmapped = F.col("ObservationType").isNotNull() & ~F.col("ObservationType").isin(list(OBS_TYPE_MAP.keys()))
add_warn(F.when(unmapped, F.concat(F.lit("UnknownObservationType:"), F.col("ObservationType"))))
 
df = df.withColumn("ObservationType", mapping_expr)
print("ObservationType normalised. Distinct values:", df.select("ObservationType").distinct().count())


# COMMAND ----------

# Step 4 — Identity cleaning
# Reject placeholder identifiers (`0`, `00`, `000`) with warning.

reject_vals = ["0", "00", "000", "NA", "N/A", "None", "NONE", "NULL", "null"]
for row in by_type.get("RejectValues", []):
    reject_vals = [v for v in pp(row["params"]).get("Values", "").split("|") if v]

for id_col in ["HerdIdentifier","AnimalIdentifier"]:
    if id_col in df.columns:
        bad = F.col(id_col).isNotNull() & F.col(id_col).isin(reject_vals)
        add_warn(F.when(bad, F.lit(f"invalidIdentifier:{id_col}")))
        df = df.withColumn(id_col, F.when(bad, None).otherwise(F.col(id_col)))

# COMMAND ----------

# Step 5 — Validate timestamps
# `EventDate` is already a proper timestamp in bronze parquet. No parsing needed — just check
# for future dates and pre-2000 dates.

MIN_TS = F.lit("2016-01-01").cast("timestamp")

if "EventDate" in df.columns:
    present   = F.col("EventDate").isNotNull()
    future_ts = present & (F.col("EventDate") > F.current_timestamp())
    old_ts    = present & (F.col("EventDate") < MIN_TS)
    add_warn(F.when(future_ts, F.lit("FutureDate:EventDate (kept)")))
    add_warn(F.when(old_ts,    F.lit("OldDate:EventDate (before 2016; set to null)")))
    df = df.withColumn("EventDate", F.when(old_ts, None).otherwise(F.col("EventDate")))

# COMMAND ----------

#  Step 6 — Cast ObservationValue to double
# Numeric cast; unparseable values become NULL with warning. Zero is valid.


if "ObservationValue" in df.columns:
    parsed = F.col("ObservationValue").cast("double")
    unparseable = F.col("ObservationValue").isNotNull() & parsed.isNull()
    add_warn(F.when(unparseable,
                F.lit("InvalidNumeric:ObservationValue (could not cast to double; set to null)")))
    df = df.withColumn("ObservationValue", parsed)

# COMMAND ----------


# Step 7 — Normalise AttributeMap keys to PascalCase
# The source data has inconsistent key casing across folders:
# `lifeNumber`, `Lifenumber`, `LifeNumber`, `numberOfBouts`, `Installationid`
# Normalise all to PascalCase so cross-dataset queries work:
# `LifeNumber`, `NumberOfBouts`, `InstallationId`, `Responder`, `Attention`

if "AttributeMap" in df.columns:
    # Step 1: lowercase all keys first (so 'Lifenumber' and 'lifeNumber' both → 'lifenumber')
    df = df.withColumn("AttributeMap",
                       F.transform_keys(F.col("AttributeMap"), lambda k, v: F.lower(k)))

    # Step 2: PascalCase mapping for known keys
    PASCAL_MAP = {
        "lifenumber":       "LifeNumber",
        "numberofbouts":    "NumberOfBouts",
        "numberofquarters": "NumberOfQuarters",
        "installationid":   "InstallationId",
        "responder":        "Responder",
        "attention":        "Attention",
        "stopdatetime":     "StopDateTime",
        "minutes":          "Minutes",
        "intervalsinceprevious":  "IntervalSincePrevious",
        "rollingaverage":         "RollingAverage",
    }

    # Build a cascading when-expression for key replacement
    def pascal_key(k, v):
        expr = k  # default: keep as-is (already lowercased)
        for lower_key, pascal in PASCAL_MAP.items():
            expr = F.when(k == lower_key, F.lit(pascal)).otherwise(expr)
        return expr

    df = df.withColumn("AttributeMap",
                       F.transform_keys(F.col("AttributeMap"), pascal_key))

    print("AttributeMap keys normalised to PascalCase")

# COMMAND ----------

# Step 8 — Required field check (warning only)


for col in required_cols:
    if col in df.columns:
        add_warn(F.when(F.col(col).isNull(), F.lit(f"MissingRequired:{col}")))

# COMMAND ----------

# Step 9 — Exact deduplication
# Duplicate = same HerdIdentifier + AnimalIdentifier + EventDate + ObservationType + ObservationValue.
# AttributeMap is not part of the dedup key.

DEDUP_REMOVED = 0

dedup_cols = [
    "HerdIdentifier",
    "AnimalIdentifier",
    "EventDate",
    "ObservationType",
    "ObservationValue"
]

before_count = df.count()
df = df.dropDuplicates(dedup_cols)
after_count = df.count()
DEDUP_REMOVED = before_count - after_count
print(f"Exact duplicate columns: {dedup_cols}")
print(f"Exact duplicates removed: {DEDUP_REMOVED:,}")

# COMMAND ----------

# Step 10 — Combine warnings
if _warn_exprs:
    df = df.withColumn(
        "Warnings",
        F.array_distinct(
            F.filter(
                F.array(*_warn_exprs),
                lambda x: x.isNotNull()
            )
        )
    )
else:
    df = df.withColumn(
        "Warnings",
        F.array().cast("array<string>")
    )
df = df.withColumn(
    "WarningCount",
    F.size("Warnings")
)
_temp_warn_cols = [c for c in df.columns if c.startswith("_w")]
if _warn_exprs:
    df = df.withColumn(
        "Warnings",
        F.array_distinct(F.filter(F.array(*_warn_exprs), lambda x: x.isNotNull()))
    )
else:
    df = df.withColumn("Warnings", F.array().cast("array<string>"))
df = df.withColumn("WarningCount", F.size("Warnings"))
df = df.drop(*_temp_warn_cols)
print("Warnings combined.")

# COMMAND ----------

# Step 11 — Audit columns

df = (df.withColumn("SourcePath",        F.lit(SRC_PATH))
        .withColumn("SilverProcessedAt", F.current_timestamp()))

# COMMAND ----------

# Step 12 — Write Delta to silver volume (partitioned by ObservationType)
# 1.2B rows, Spark manages file count per partition naturally.
# Partitioned so downstream queries on one measurement type don't scan the whole table.

df = anonymise(df, "SensorData")
(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema","true")
   .partitionBy("ObservationType")
   .save(DATA_PATH))

SILVER_ROWS = spark.read.format("delta").load(DATA_PATH).count()
print(f"WROTE {SILVER_ROWS:,} rows  ->  {DATA_PATH}")
try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} USING DELTA LOCATION '{DATA_PATH}'")
    print("Table registered:", TARGET_TABLE)
except Exception as e:
    print(f"Table registration skipped. Read: spark.read.format('delta').load('{DATA_PATH}')\n{e}")

# COMMAND ----------


# Step 13 — Run summary
silver = spark.read.format("delta").load(DATA_PATH)
wexp   = silver.select(F.explode("Warnings").alias("w"))
def cnt(pref): return wexp.where(F.col("w").startswith(pref)).count()

ROWS_WITH_WARN = silver.where(F.col("WarningCount") > 0).count()

# per-ObservationType counts
print("Rows per ObservationType:")
display(silver.groupBy("ObservationType").count().orderBy(F.desc("count")))

summary = [
    ("bronzeRows",                          BRONZE_ROWS),
    ("exactDuplicatesRemoved",              DEDUP_REMOVED),
    ("silverRows",                          SILVER_ROWS),
    ("rowsClean",                           SILVER_ROWS - ROWS_WITH_WARN),
    ("rowsWithWarnings",                    ROWS_WITH_WARN),
    ("InvalidIdentifier (nulled)",          cnt("InvalidIdentifier")),
    ("FutureDate (kept)",                   cnt("FutureDate")),
    ("OldDate (nulled)",                    cnt("OldDate")),
    ("InvalidNumeric (nulled)",             cnt("InvalidNumeric")),
    ("MissingRequired",                     cnt("MissingRequired")),
    ("UnknownObservationType (kept+warned)",cnt("UnknownObservationType")),
]

print("=" * 52)
print("  SOS SensorData — Silver Run Summary")
print("=" * 52)
for k, v in summary:
    print(f"  {k:<36} {v:>15,}")

display(spark.createDataFrame(summary, ["metric","count"]))


display(wexp.groupBy("w").count().orderBy(F.desc("count")))
display(silver.limit(20))

# COMMAND ----------

DATA_PATH    = "/Volumes/senseofsensors_catalog/silver/silver_volume/SensorData"
SILVER_SCHEMA = "senseofsensors_catalog.silver"

silver = spark.read.format("delta").load(DATA_PATH)

# 1. Basic shape
print(f"Total rows:    {silver.count():,}")
print(f"Total columns: {len(silver.columns)}")
print(f"Columns: {silver.columns}")

# 2. No Raw columns leaked
raw_cols = [c for c in silver.columns if c.endswith("Raw")]
print(f"\nRaw columns (should be 0): {len(raw_cols)} — {raw_cols}")

# 3. Row count per ObservationType (all 23 should appear)
print("\nRows per ObservationType:")
display(silver.groupBy("ObservationType").count().orderBy("ObservationType"))

# 4. ObservationValue — should be double, check for nulls
from pyspark.sql import functions as F
ov = silver.select(
    F.count("ObservationValue").alias("nonNull"),
    F.count(F.when(F.col("ObservationValue").isNull(), 1)).alias("nullCount"),
    F.min("ObservationValue").alias("min"),
    F.max("ObservationValue").alias("max"),
)
print("\nObservationValue stats:")
display(ov)

# 5. Identity columns — check for nulls
print("\nNull counts on required identity columns:")
display(silver.select(
    F.count(F.when(F.col("HerdIdentifier").isNull(), 1)).alias("HerdIdentifier_nulls"),
    F.count(F.when(F.col("AnimalIdentifier").isNull(), 1)).alias("AnimalIdentifier_nulls"),
    F.count(F.when(F.col("EventDate").isNull(), 1)).alias("EventDate_nulls"),
    F.count(F.when(F.col("ObservationType").isNull(), 1)).alias("ObservationType_nulls"),
))

# 6. AttributeMap key casing — confirm PascalCase normalisation
print("\nSample AttributeMap keys (should all be PascalCase):")
display(silver
    .select(F.explode(F.map_keys(F.col("AttributeMap"))).alias("key"))
    .groupBy("key").count()
    .orderBy(F.desc("count"))
    .limit(20))

# 7. Warnings summary
wexp = silver.select(F.explode("Warnings").alias("w"))
print(f"\nRows with warnings: {silver.where(F.col('WarningCount') > 0).count():,}")
display(wexp.groupBy("w").count().orderBy(F.desc("count")).limit(20))

# 8. Date range sanity check
print("\nEventDate range:")
display(silver.select(
    F.min("EventDate").alias("earliest"),
    F.max("EventDate").alias("latest"),
))

# COMMAND ----------

#are the duplicates concentrated in specific types?
display(
    silver
    .where(F.col("WarningCount") > 0)
    .groupBy("ObservationType")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
)

# confirm the AttributeMap keys are PascalCase
display(
    silver
    .select(F.explode(F.map_keys(F.col("AttributeMap"))).alias("key"))
    .groupBy("key").count()
    .orderBy(F.desc("count"))
    .limit(15)
)