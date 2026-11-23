# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG       = dbutils.widgets.get("catalog")
SILVER_VOLUME = dbutils.widgets.get("silver_volume").rstrip("/")
GOLD_SCHEMA   = f"{CATALOG}.gold"
SILVER_SCHEMA = f"{CATALOG}.silver"

SENSOR_PATH = f"{SILVER_VOLUME}/SensorData"
REF_PATH    = f"{SILVER_VOLUME}/ReferenceData"
OUT_TABLE   = f"{GOLD_SCHEMA}.AnimalIdentityBridge"

print("Reading sensor from :", SENSOR_PATH)
print("Reading ref from    :", REF_PATH)
print("Writing to          :", OUT_TABLE)

# COMMAND ----------

# From SensorData: extract LifeNumber from AttributeMap
# Only need the identity columns
sensor_ids = (spark.read.format("delta").load(SENSOR_PATH)
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        F.col("AttributeMap")["LifeNumber"].alias("SensorLifeNumber")
    )
    .distinct())

# From ReferenceData: identity columns with LifeNumberDigits
ref_ids = (spark.read.format("delta").load(REF_PATH)
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        "LifeNumber",
        "LifeNumberDigits"
    )
    .distinct())

print(f"Distinct sensor identity combinations : {sensor_ids.count():,}")
print(f"Distinct ref identity combinations    : {ref_ids.count():,}")

# COMMAND ----------

# Strip all non-digit characters from SensorLifeNumber
# This handles: "NL 123456789" -> "123456789"
#               "DE 0359941964" -> "0359941964"
#               "644018250"     -> "644018250"
#               missing space "NL123456789" -> "123456789"

sensor_ids = sensor_ids.withColumn(
    "SensorLifeNumberDigits",
    F.when(
        F.col("SensorLifeNumber").isNotNull(),
        F.regexp_replace(F.col("SensorLifeNumber"), r"\D", "")
    )
)

print("Sample sensor identities after normalisation:")
display(sensor_ids.limit(10))

# COMMAND ----------

# Join strategy:
#   Primary key  : SensorLifeNumberDigits == LifeNumberDigits + HerdIdentifier
#   Fallback key : AnimalIdentifier + HerdIdentifier (for rows with no LifeNumber in AttributeMap)
# All sensor animals kept (left join) — unmatched get null ref columns

s = sensor_ids.alias("s")
r = ref_ids.alias("r")

# Primary join — LifeNumber digits + HerdIdentifier
primary = s.join(
    r,
    (F.col("s.SensorLifeNumberDigits") == F.col("r.LifeNumberDigits")) &
    (F.col("s.HerdIdentifier")         == F.col("r.HerdIdentifier")),
    "left"
).select(
    F.col("s.HerdIdentifier"),
    F.col("s.AnimalIdentifier").alias("SensorAnimalIdentifier"),
    F.col("s.SensorLifeNumber"),
    F.col("s.SensorLifeNumberDigits"),
    F.col("r.LifeNumber").alias("RefLifeNumber"),
    F.col("r.LifeNumberDigits").alias("RefLifeNumberDigits"),
    F.col("r.AnimalIdentifier").alias("RefAnimalIdentifier"),
    F.lit("LifeNumber").alias("JoinMethod")
)

matched_primary   = primary.where(F.col("RefLifeNumber").isNotNull()).count()
unmatched_primary = primary.where(F.col("RefLifeNumber").isNull()).count()

print(f"Primary join (LifeNumber digits) :")
print(f"  Matched   : {matched_primary:,}")
print(f"  Unmatched : {unmatched_primary:,}")

# COMMAND ----------

# For sensor animals with no LifeNumber in AttributeMap,
# or whose LifeNumber truly doesn't exist in ReferenceData,
# try AnimalIdentifier + HerdIdentifier as fallback

unmatched = primary.where(F.col("RefLifeNumber").isNull()).alias("u")
ref_by_animal = ref_ids.alias("ra")

fallback = unmatched.join(
    ref_by_animal,
    (F.col("u.SensorAnimalIdentifier") == F.col("ra.AnimalIdentifier")) &
    (F.col("u.HerdIdentifier")         == F.col("ra.HerdIdentifier")),
    "left"
).select(
    F.col("u.HerdIdentifier"),
    F.col("u.SensorAnimalIdentifier"),
    F.col("u.SensorLifeNumber"),
    F.col("u.SensorLifeNumberDigits"),
    F.col("ra.LifeNumber").alias("RefLifeNumber"),
    F.col("ra.LifeNumberDigits").alias("RefLifeNumberDigits"),
    F.col("ra.AnimalIdentifier").alias("RefAnimalIdentifier"),
    F.when(
        F.col("ra.AnimalIdentifier").isNotNull(),
        F.lit("AnimalIdentifier")
    ).otherwise(F.lit("NoMatch")).alias("JoinMethod")
)

matched_fallback   = fallback.where(F.col("RefLifeNumber").isNotNull()).count()
unmatched_fallback = fallback.where(F.col("RefLifeNumber").isNull()).count()

print(f"Fallback join (AnimalIdentifier) :")
print(f"  Additionally matched : {matched_fallback:,}")
print(f"  Still unmatched      : {unmatched_fallback:,}")

# COMMAND ----------

# Union the primary matched rows with the fallback results
# This gives one row per distinct sensor animal identity combination

matched_rows = primary.where(F.col("RefLifeNumber").isNotNull())
bridge = matched_rows.unionByName(fallback)

total        = bridge.count()
matched      = bridge.where(F.col("JoinMethod") != "NoMatch").count()
no_match     = bridge.where(F.col("JoinMethod") == "NoMatch").count()
by_lifenumber   = bridge.where(F.col("JoinMethod") == "LifeNumber").count()
by_animalid     = bridge.where(F.col("JoinMethod") == "AnimalIdentifier").count()

print(f"Bridge table total rows     : {total:,}")
print(f"Matched via LifeNumber      : {by_lifenumber:,}  ({by_lifenumber/total*100:.1f}%)")
print(f"Matched via AnimalIdentifier: {by_animalid:,}  ({by_animalid/total*100:.1f}%)")
print(f"No match (sensor only)      : {no_match:,}  ({no_match/total*100:.1f}%)")
print()
print("No-match sample (sensor animals with no ReferenceData record):")
display(bridge
    .where(F.col("JoinMethod") == "NoMatch")
    .select("HerdIdentifier", "SensorAnimalIdentifier", "SensorLifeNumber")
    .limit(20))

# COMMAND ----------

# GoldMaster will join SensorData to LactationWindows using one consistent key.
# BestLifeNumberDigits = RefLifeNumberDigits if matched, else SensorLifeNumberDigits

bridge = bridge.withColumn(
    "BestLifeNumberDigits",
    F.coalesce(F.col("RefLifeNumberDigits"), F.col("SensorLifeNumberDigits"))
).withColumn(
    "HasRefData",
    F.col("JoinMethod") != "NoMatch"
)

print("Sample bridge rows:")
display(bridge.orderBy("HerdIdentifier", "SensorAnimalIdentifier").limit(20))

# COMMAND ----------

print("Bridge coverage per farm:")
display(bridge
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("TotalSensorAnimals"),
        F.sum(F.col("HasRefData").cast("int")).alias("MatchedToRefData"),
        F.sum((F.col("JoinMethod") == "NoMatch").cast("int")).alias("SensorOnly")
    )
    .withColumn("MatchPct",
        F.round(F.col("MatchedToRefData") / F.col("TotalSensorAnimals") * 100, 1))
    .orderBy("HerdIdentifier"))

print("\nJoin method breakdown:")
display(bridge.groupBy("JoinMethod").count().orderBy("JoinMethod"))

print("\nSensor animals with no LifeNumber in AttributeMap:")
no_ln = bridge.where(F.col("SensorLifeNumber").isNull()).count()
print(f"  {no_ln:,}")

# COMMAND ----------

(bridge
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")

# COMMAND ----------

gold_bridge = spark.table(OUT_TABLE)

print("=" * 55)
print("  Gold 2 — AnimalIdentityBridge Summary")
print("=" * 55)
print(f"  Total rows              : {gold_bridge.count():>10,}")
print(f"  Matched to ReferenceData: {gold_bridge.where(F.col('HasRefData')==True).count():>10,}")
print(f"  Sensor only (no match)  : {gold_bridge.where(F.col('HasRefData')==False).count():>10,}")
print(f"  Columns                 : {len(gold_bridge.columns):>10,}")
print()
print("Schema:")
gold_bridge.printSchema()

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"
bridge = spark.table("senseofsensors_catalog.gold.AnimalIdentityBridge")
(bridge.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_VOLUME}/AnimalIdentityBridge"))
print("Written to Volume path")

# COMMAND ----------

display(spark.read.format("delta").load(f"{GOLD_VOLUME}/AnimalIdentityBridge")
    .select("HerdIdentifier").distinct().orderBy("HerdIdentifier"))