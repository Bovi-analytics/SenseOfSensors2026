# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG       = dbutils.widgets.get("catalog")
SILVER_VOLUME = dbutils.widgets.get("silver_volume").rstrip("/")
GOLD_SCHEMA   = f"{CATALOG}.gold"

SENSOR_PATH = f"{SILVER_VOLUME}/SensorData"
OUT_TABLE   = f"{GOLD_SCHEMA}.DailyBehaviourSummary"

print("Reading from :", SENSOR_PATH)
print("Writing to   :", OUT_TABLE)

# COMMAND ----------

sensor = (spark.read.format("delta").load(SENSOR_PATH)
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        "ObservationType",
        "ObservationValue",
        F.to_date("EventDate").alias("EventDay")
    ))

print(f"Total sensor rows : {sensor.count():,}")
print(f"Date range        : {sensor.select(F.min('EventDay').alias('Min'), F.max('EventDay').alias('Max')).first()}")

# COMMAND ----------

# 3-standard-deviation outlier filter per ObservationType
# Only applies to numeric ObservationValue

summary_stats = (sensor
    .where(F.col("ObservationValue").isNotNull())
    .groupBy("ObservationType")
    .agg(
        F.avg("ObservationValue").alias("AvgValue"),
        F.stddev_pop("ObservationValue").alias("StddevValue")
    ))

sensor_filtered = (sensor
    .join(F.broadcast(summary_stats), "ObservationType", "left")
    .where(
        F.col("ObservationValue").isNull() |
        F.col("StddevValue").isNull() |
        (F.abs(F.col("ObservationValue") - F.col("AvgValue")) <= 3 * F.col("StddevValue"))
    )
    .drop("AvgValue", "StddevValue")
)

before = sensor.count()
after  = sensor_filtered.count()
print(f"Before outlier filter : {before:,}")
print(f"After outlier filter  : {after:,}")
print(f"Outliers removed      : {before - after:,}  ({(before-after)/before*100:.2f}%)")

# Replace sensor with filtered version for all subsequent cells
sensor = sensor_filtered

# COMMAND ----------

# Values are already in minutes
# LegActivityData is a step count — sum as-is

OBS_15MIN = [
    "EatingTimeData15min",
    "RuminatingTimeData15min",
    "InactiveTimeData15min",
    "LyingTimeData",
    "StandingTimeData",
    "WalkingTimeData",
]

sensor_15min = sensor.where(
    F.col("ObservationType").isin(OBS_15MIN + ["LegActivityData"]))

daily_15min = (sensor_15min
    .groupBy("HerdIdentifier", "AnimalIdentifier", "EventDay")
    .agg(
        F.sum(F.when(F.col("ObservationType") == "EatingTimeData15min",
                     F.col("ObservationValue"))).alias("DailyEatingMin15"),

        F.sum(F.when(F.col("ObservationType") == "RuminatingTimeData15min",
                     F.col("ObservationValue"))).alias("DailyRuminatingMin15"),

        F.sum(F.when(F.col("ObservationType") == "InactiveTimeData15min",
                     F.col("ObservationValue"))).alias("DailyInactiveMin15"),

        F.sum(F.when(F.col("ObservationType") == "LyingTimeData",
                     F.col("ObservationValue"))).alias("DailyLyingMin15"),

        F.sum(F.when(F.col("ObservationType") == "StandingTimeData",
                     F.col("ObservationValue"))).alias("DailyStandingMin15"),

        F.sum(F.when(F.col("ObservationType") == "WalkingTimeData",
                     F.col("ObservationValue"))).alias("DailyWalkingMin15"),

        F.sum(F.when(F.col("ObservationType") == "LegActivityData",
                     F.col("ObservationValue"))).alias("DailyLegActivity"),

        F.count(F.when(F.col("ObservationType") == "EatingTimeData15min",
                       F.lit(1))).alias("Eating15minWindows"),
        F.count(F.when(F.col("ObservationType") == "LyingTimeData",
                       F.lit(1))).alias("Lying15minWindows"),
    ))

print(f"Daily 15-min rows : {daily_15min.count():,}")
display(daily_15min.limit(5))

# COMMAND ----------

# Each 2-hr window value represents 120 minutes of time.
# Sum per animal per day and multiply by 120 to get daily minutes.

# Values are already in minutes (0-120 per 2-hr window) — sum directly, no multiplication

OBS_2HR = [
    "EatingTimeData2hr",
    "RuminatingTimeData2hr",
    "InactiveTimeData2hr",
]

sensor_2hr = sensor.where(F.col("ObservationType").isin(OBS_2HR))

daily_2hr = (sensor_2hr
    .groupBy("HerdIdentifier", "AnimalIdentifier", "EventDay")
    .agg(
        F.sum(F.when(F.col("ObservationType") == "EatingTimeData2hr",
                     F.col("ObservationValue"))).alias("DailyEatingMin2hr"),

        F.sum(F.when(F.col("ObservationType") == "RuminatingTimeData2hr",
                     F.col("ObservationValue"))).alias("DailyRuminatingMin2hr"),

        F.sum(F.when(F.col("ObservationType") == "InactiveTimeData2hr",
                     F.col("ObservationValue"))).alias("DailyInactiveMin2hr"),

        F.count(F.when(F.col("ObservationType") == "EatingTimeData2hr",
                       F.lit(1))).alias("Eating2hrWindows"),
    ))

print(f"Daily 2-hr rows : {daily_2hr.count():,}")
display(daily_2hr.limit(5))

# COMMAND ----------

# These are already daily values — one per animal per day.
# Pivot each ObservationType into its own column.

OBS_DAILY = [
    "EatingTimeDataDay",
    "RuminatingTimeDataDay",
    "InactiveTimeDataDay",
    "LyingBoutData",
    "LyingBoutSummaryMeanBoutLength",
    "LyingBoutSummaryMedianBoutLength",
    "LyingBoutSummaryMaxBoutLength",
    "LyingBoutSummaryMinBoutLength",
    "LyingBoutSummaryNrOfBouts",
    "LyingBoutSummaryLowerQuartileBoutLength",
    "LyingBoutSummaryUpperQuartileBoutLength",
    "StandupSummary",
    "LyingSummary",
    "StandingSummary",
    "WalkingSummary",
    "LegActivitySummary",
]

sensor_daily = sensor.where(F.col("ObservationType").isin(OBS_DAILY))

daily_summary = (sensor_daily
    .groupBy("HerdIdentifier", "AnimalIdentifier", "EventDay")
    .agg(
        F.first(F.when(F.col("ObservationType") == "EatingTimeDataDay",
                       F.col("ObservationValue"))).alias("DailyEatingMinDay"),
        F.first(F.when(F.col("ObservationType") == "RuminatingTimeDataDay",
                       F.col("ObservationValue"))).alias("DailyRuminatingMinDay"),
        F.first(F.when(F.col("ObservationType") == "InactiveTimeDataDay",
                       F.col("ObservationValue"))).alias("DailyInactiveMinDay"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutData",
                       F.col("ObservationValue"))).alias("LyingBouts"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryMeanBoutLength",
                       F.col("ObservationValue"))).alias("MeanLyingBoutLength"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryMedianBoutLength",
                       F.col("ObservationValue"))).alias("MedianLyingBoutLength"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryMaxBoutLength",
                       F.col("ObservationValue"))).alias("MaxLyingBoutLength"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryMinBoutLength",
                       F.col("ObservationValue"))).alias("MinLyingBoutLength"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryNrOfBouts",
                       F.col("ObservationValue"))).alias("NrLyingBouts"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryLowerQuartileBoutLength",
                       F.col("ObservationValue"))).alias("LyingBoutLowerQuartile"),
        F.first(F.when(F.col("ObservationType") == "LyingBoutSummaryUpperQuartileBoutLength",
                       F.col("ObservationValue"))).alias("LyingBoutUpperQuartile"),
        F.first(F.when(F.col("ObservationType") == "StandupSummary",
                       F.col("ObservationValue"))).alias("StandupCount"),
        F.first(F.when(F.col("ObservationType") == "LyingSummary",
                       F.col("ObservationValue"))).alias("LyingSummary"),
        F.first(F.when(F.col("ObservationType") == "StandingSummary",
                       F.col("ObservationValue"))).alias("StandingSummary"),
        F.first(F.when(F.col("ObservationType") == "WalkingSummary",
                       F.col("ObservationValue"))).alias("WalkingSummary"),
        F.first(F.when(F.col("ObservationType") == "LegActivitySummary",
                       F.col("ObservationValue"))).alias("LegActivitySummary"),
    ))

print(f"Daily summary rows : {daily_summary.count():,}")
display(daily_summary.limit(5))

# COMMAND ----------

SILVER_VOLUME = "/Volumes/senseofsensors_catalog/silver/silver_volume"

sensor_raw = spark.read.format("delta").load(f"{SILVER_VOLUME}/SensorData")

# Count rows per ObservationType where value > 15 (max possible for 15-min window)
# A single 15-min window can have max 15 minutes of any activity
OBS_15MIN = [
    "EatingTimeData15min", "RuminatingTimeData15min", "InactiveTimeData15min",
    "LyingTimeData", "StandingTimeData", "WalkingTimeData"
]

print("Raw silver rows with impossible values per 15-min ObservationType:")
display(sensor_raw
    .where(F.col("ObservationType").isin(OBS_15MIN))
    .groupBy("ObservationType")
    .agg(
        F.count("*").alias("TotalRows"),
        F.sum((F.col("ObservationValue") > 15).cast("int")).alias("Over15"),
        F.round(F.sum((F.col("ObservationValue") > 15).cast("int")) / F.count("*") * 100, 3)
         .alias("PctOver15")
    )
    .orderBy("ObservationType"))

print("\nHow many distinct animals have ANY reading > 15 in silver:")
display(sensor_raw
    .where(
        F.col("ObservationType").isin(OBS_15MIN) &
        (F.col("ObservationValue") > 15)
    )
    .groupBy("HerdIdentifier")
    .agg(
        F.countDistinct("AnimalIdentifier").alias("AffectedAnimals"),
        F.count("*").alias("AffectedRows")
    )
    .orderBy("HerdIdentifier"))

# COMMAND ----------

# How many animals have more than 96 15-min windows in a single day?
display(sensor_raw
    .where(F.col("ObservationType") == "EatingTimeData15min")
    .groupBy("HerdIdentifier", "AnimalIdentifier", F.to_date("EventDate").alias("EventDay"))
    .count()
    .where(F.col("count") > 96)
    .groupBy("HerdIdentifier")
    .agg(
        F.countDistinct("AnimalIdentifier").alias("AffectedAnimals"),
        F.count("*").alias("AffectedDays"),
        F.max("count").alias("MaxWindowsInOneDay")
    )
    .orderBy("HerdIdentifier"))

# COMMAND ----------

JOIN_KEYS = ["HerdIdentifier", "AnimalIdentifier", "EventDay"]

daily_behaviour = (daily_15min
    .join(daily_2hr,     JOIN_KEYS, "full")
    .join(daily_summary, JOIN_KEYS, "full")
    # Null impossible daily totals (> 1440 minutes)
    # Caused by animals with >96 windows in a day due to near-duplicate timestamps
    # 0.22% of animal-days affected — nulled not capped
    .withColumn("DailyEatingMin15",
        F.when(F.col("DailyEatingMin15") > 1440, None)
         .otherwise(F.col("DailyEatingMin15")))
    .withColumn("DailyRuminatingMin15",
        F.when(F.col("DailyRuminatingMin15") > 1440, None)
         .otherwise(F.col("DailyRuminatingMin15")))
    .withColumn("DailyInactiveMin15",
        F.when(F.col("DailyInactiveMin15") > 1440, None)
         .otherwise(F.col("DailyInactiveMin15")))
    .withColumn("DailyLyingMin15",
        F.when(F.col("DailyLyingMin15") > 1440, None)
         .otherwise(F.col("DailyLyingMin15")))
    .withColumn("DailyStandingMin15",
        F.when(F.col("DailyStandingMin15") > 1440, None)
         .otherwise(F.col("DailyStandingMin15")))
    .withColumn("DailyWalkingMin15",
        F.when(F.col("DailyWalkingMin15") > 1440, None)
         .otherwise(F.col("DailyWalkingMin15")))
    .withColumn("DailyEatingMin2hr",
        F.when(F.col("DailyEatingMin2hr") > 1440, None)
         .otherwise(F.col("DailyEatingMin2hr")))
    .withColumn("DailyRuminatingMin2hr",
        F.when(F.col("DailyRuminatingMin2hr") > 1440, None)
         .otherwise(F.col("DailyRuminatingMin2hr")))
    .withColumn("DailyInactiveMin2hr",
        F.when(F.col("DailyInactiveMin2hr") > 1440, None)
         .otherwise(F.col("DailyInactiveMin2hr")))
)

print(f"Combined daily behaviour rows : {daily_behaviour.count():,}")
print(f"Columns                       : {len(daily_behaviour.columns)}")

# COMMAND ----------

# Quick check on in-memory dataframe before overwriting
display(daily_behaviour
    .where(F.col("DailyEatingMin15").isNotNull())
    .select(
        F.max("DailyEatingMin15").alias("MaxEating15"),
        F.max("DailyLyingMin15").alias("MaxLying15"),
        F.max("DailyEatingMin2hr").alias("MaxEating2hr"),
    ))

# COMMAND ----------

(daily_behaviour
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("HerdIdentifier")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")
print(f"Partitioned by HerdIdentifier (8 partitions)")

# COMMAND ----------

gold_dbs = spark.table(OUT_TABLE)

print("=" * 55)
print("  Gold 3 — DailyBehaviourSummary Validation")
print("=" * 55)

# 1. Row count
total = gold_dbs.count()
print(f"\n  Total animal-days     : {total:>12,}")
print(f"  Distinct animals      : {gold_dbs.select('AnimalIdentifier').distinct().count():>12,}")
print(f"  Distinct farms        : {gold_dbs.select('HerdIdentifier').distinct().count():>12,}")
print(f"  Distinct days         : {gold_dbs.select('EventDay').distinct().count():>12,}")
print(f"  Columns               : {len(gold_dbs.columns):>12,}")

# 2. No duplicate animal-days
dupes = gold_dbs.groupBy("HerdIdentifier", "AnimalIdentifier", "EventDay").count().where(F.col("count") > 1).count()
print(f"\n  Duplicate animal-days : {dupes:>12,}  {' ' if dupes == 0 else '❌ PROBLEM'}")

# 3. Date range
print("\n  Date range:")
display(gold_dbs.select(
    F.min("EventDay").alias("EarliestDay"),
    F.max("EventDay").alias("LatestDay")
))

# 4. Rows per farm
print("\n  Rows per farm:")
display(gold_dbs
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("AnimalDays"),
        F.countDistinct("AnimalIdentifier").alias("Animals"),
        F.countDistinct("EventDay").alias("Days")
    )
    .orderBy("HerdIdentifier"))

# 5. Value sanity check — daily minutes should be between 0 and 1440 (24hrs)
print("\n  Values out of range (> 1440 minutes per day):")
display(gold_dbs.select(
    F.sum((F.col("DailyEatingMin15")    > 1440).cast("int")).alias("EatingMin15_Over1440"),
    F.sum((F.col("DailyRuminatingMin15")> 1440).cast("int")).alias("RuminatingMin15_Over1440"),
    F.sum((F.col("DailyLyingMin15")     > 1440).cast("int")).alias("LyingMin15_Over1440"),
    F.sum((F.col("DailyEatingMin2hr")   > 1440).cast("int")).alias("EatingMin2hr_Over1440"),
))

# 6. Null rates
print("\n  Null rates on key columns:")
display(gold_dbs.select(
    F.round(F.sum(F.col("DailyEatingMin15").isNull().cast("int"))     / total * 100, 1).alias("EatingMin15_NullPct"),
    F.round(F.sum(F.col("DailyLyingMin15").isNull().cast("int"))      / total * 100, 1).alias("LyingMin15_NullPct"),
    F.round(F.sum(F.col("DailyEatingMin2hr").isNull().cast("int"))    / total * 100, 1).alias("EatingMin2hr_NullPct"),
    F.round(F.sum(F.col("LyingBouts").isNull().cast("int"))           / total * 100, 1).alias("LyingBouts_NullPct"),
    F.round(F.sum(F.col("MeanLyingBoutLength").isNull().cast("int"))  / total * 100, 1).alias("MeanLyingBout_NullPct"),
))

# 7. Spot check — one animal across time
_spot = gold_dbs.select("HerdIdentifier", "AnimalIdentifier").first()
print(f"\n  Spot check — animal {_spot.AnimalIdentifier} on {_spot.HerdIdentifier}:")
display(gold_dbs
    .where((F.col("HerdIdentifier") == _spot.HerdIdentifier) & (F.col("AnimalIdentifier") == _spot.AnimalIdentifier))
    .orderBy("EventDay")
    .limit(10))

# COMMAND ----------

# Quick spot-check — should show Farm_A…Farm_H, never numeric IDs
gold_dbs = spark.table("senseofsensors_catalog.gold.DailyBehaviourSummary")

print("Row count:", gold_dbs.count())
print("\nDistinct HerdIdentifiers:")
gold_dbs.select("HerdIdentifier").distinct().show()

print("\nSample rows for Farm_A:")
display(gold_dbs
    .where(F.col("HerdIdentifier") == "Farm_A")
    .orderBy("EventDay")
    .limit(10))

# COMMAND ----------

# Copy to Volume path 
GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"

dbs = spark.table("senseofsensors_catalog.gold.DailyBehaviourSummary")
(dbs.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_VOLUME}/DailyBehaviourSummary"))

print("Done. Verifying Volume path...")

#Confirm Volume path
spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyBehaviourSummary") \
    .select("HerdIdentifier").distinct().show()