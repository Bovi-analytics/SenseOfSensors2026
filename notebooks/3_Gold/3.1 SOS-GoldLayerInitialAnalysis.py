# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG       = "senseofsensors_catalog"
SILVER_SCHEMA = f"{CATALOG}.silver"
SILVER_VOLUME = "/Volumes/senseofsensors_catalog/silver/silver_volume"

# Load all three silver tables
silver_ref    = spark.read.format("delta").load(f"{SILVER_VOLUME}/ReferenceData")
silver_thi    = spark.read.format("delta").load(f"{SILVER_VOLUME}/TemperatureHumidityIndex")
silver_sensor = spark.read.format("delta").load(f"{SILVER_VOLUME}/SensorData")

print(f"ReferenceData rows : {silver_ref.count():>15,}")
print(f"THI rows           : {silver_thi.count():>15,}")
print(f"SensorData rows    : {silver_sensor.count():>15,}")

# COMMAND ----------

#HerdIdentifier across all 3 datasets
ref_herds    = set(r[0] for r in silver_ref.select("HerdIdentifier").distinct().collect())
thi_herds    = set(r[0] for r in silver_thi.select("HerdIdentifier").distinct().collect())
sensor_herds = set(r[0] for r in silver_sensor.select("HerdIdentifier").distinct().collect())

print(f"ReferenceData farms : {sorted(ref_herds)}")
print(f"THI farms           : {sorted(thi_herds)}")
print(f"SensorData farms    : {sorted(sensor_herds)}")
print()
print(f"In RefData  not in THI    : {ref_herds - thi_herds}")
print(f"In RefData  not in Sensor : {ref_herds - sensor_herds}")
print(f"In THI      not in Sensor : {thi_herds - sensor_herds}")
print(f"In Sensor   not in RefData: {sensor_herds - ref_herds}")
print(f"In Sensor   not in THI    : {sensor_herds - thi_herds}")
print()
all_match = (ref_herds == thi_herds == sensor_herds)
print(f"All three match exactly: {all_match}")

# COMMAND ----------

# What does LifeNumber actually look like inside the AttributeMap

sensor_ln = (silver_sensor
    .select(F.col("AttributeMap")["LifeNumber"].alias("SensorLifeNumber"))
    .where(F.col("SensorLifeNumber").isNotNull()))

total_with_ln = sensor_ln.count()
distinct_ln   = sensor_ln.distinct().count()

print(f"Rows with LifeNumber in AttributeMap : {total_with_ln:,}")
print(f"Distinct LifeNumber values           : {distinct_ln:,}")
print()
print("Sample values:")
display(sensor_ln.distinct().limit(20))

# COMMAND ----------

#Lifenumber in reference data
print("ReferenceData LifeNumber samples:")
display(silver_ref
    .select("HerdIdentifier", "AnimalIdentifier", "LifeNumber", "LifeNumberDigits")
    .where(F.col("LifeNumber").isNotNull())
    .limit(20))

print(f"\nDistinct LifeNumbers in ReferenceData   : {silver_ref.select('LifeNumber').distinct().count():,}")
print(f"Distinct LifeNumberDigits in ReferenceData: {silver_ref.select('LifeNumberDigits').distinct().count():,}")


# COMMAND ----------

# matching SensorData LifeNumber directly to ReferenceData LifeNumber
sensor_ln_df = (silver_sensor
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        F.col("AttributeMap")["LifeNumber"].alias("SensorLifeNumber")
    )
    .where(F.col("SensorLifeNumber").isNotNull())
    .distinct())

ref_ln_df = (silver_ref
    .select("HerdIdentifier", "LifeNumber", "LifeNumberDigits", "AnimalIdentifier")
    .where(F.col("LifeNumber").isNotNull())
    .distinct())

# Alias both sides to avoid ambiguous column references
s = sensor_ln_df.alias("s")
r = ref_ln_df.alias("r")

exact_match = s.join(
    r,
    (F.col("s.SensorLifeNumber") == F.col("r.LifeNumber")) &
    (F.col("s.HerdIdentifier")   == F.col("r.HerdIdentifier")),
    "left"
)

total     = sensor_ln_df.count()
matched   = exact_match.where(F.col("r.LifeNumber").isNotNull()).count()
unmatched = exact_match.where(F.col("r.LifeNumber").isNull()).count()

print(f"Distinct SensorData animals with LifeNumber : {total:,}")
print(f"Matched to ReferenceData (exact)            : {matched:,}  ({matched/total*100:.1f}%)")
print(f"Unmatched                                   : {unmatched:,}  ({unmatched/total*100:.1f}%)")
print()
print("Unmatched SensorData LifeNumbers (sample):")
display(exact_match
    .where(F.col("r.LifeNumber").isNull())
    .select(
        F.col("s.HerdIdentifier"),
        F.col("s.AnimalIdentifier"),
        F.col("s.SensorLifeNumber")
    )
    .limit(20))

# COMMAND ----------

# do the unmatched sensor LifeNumbers at least have digits 
# that appear somewhere in ReferenceData LifeNumberDigits?

unmatched_df = (exact_match
    .where(F.col("r.LifeNumber").isNull())
    .select(
        F.col("s.HerdIdentifier"),
        F.col("s.AnimalIdentifier"),
        F.col("s.SensorLifeNumber"),
        F.regexp_replace(F.col("s.SensorLifeNumber"), r"\D", "").alias("SensorDigitsOnly")
    )).alias("u")

ref_digits = (silver_ref
    .select("LifeNumberDigits")
    .where(F.col("LifeNumberDigits").isNotNull())
    .distinct()
    .alias("rd"))

digits_coverage = unmatched_df.join(
    ref_digits,
    F.col("u.SensorDigitsOnly") == F.col("rd.LifeNumberDigits"),
    "left"
)

could_match  = digits_coverage.where(F.col("rd.LifeNumberDigits").isNotNull()).count()
truly_absent = digits_coverage.where(F.col("rd.LifeNumberDigits").isNull()).count()
total_unmatched = unmatched_df.count()

print(f"Unmatched sensor animals total    : {total_unmatched:,}")
print(f"Would match on digits only        : {could_match:,}  ({could_match/total_unmatched*100:.1f}%)")
print(f"Truly absent from ReferenceData   : {truly_absent:,}  ({truly_absent/total_unmatched*100:.1f}%)")
print()
print("Sample of truly absent animals:")
display(digits_coverage
    .where(F.col("rd.LifeNumberDigits").isNull())
    .select(
        F.col("u.HerdIdentifier"),
        F.col("u.SensorLifeNumber")
    )
    .distinct()
    .limit(20))

print("\nAbsent animals per farm:")
display(digits_coverage
    .where(F.col("rd.LifeNumberDigits").isNull())
    .groupBy(F.col("u.HerdIdentifier"))
    .agg(F.countDistinct("u.SensorLifeNumber").alias("AbsentAnimals"))
    .orderBy("u.HerdIdentifier"))

# COMMAND ----------

# If exact match failed or was partial, try matching on digits only
# Strip non-digits from SensorLifeNumber and compare to LifeNumberDigits

sensor_digits = (sensor_ln_df
    .withColumn(
        "SensorDigitsOnly",
        F.regexp_replace(F.col("SensorLifeNumber"), r"\D", "")
    ))

ref_digits = (silver_ref
    .select("HerdIdentifier", "LifeNumberDigits")
    .where(F.col("LifeNumberDigits").isNotNull())
    .distinct())

sd = sensor_digits.alias("sd")
rd = ref_digits.alias("rd")

digits_match = sd.join(
    rd,
    (F.col("sd.SensorDigitsOnly") == F.col("rd.LifeNumberDigits")) &
    (F.col("sd.HerdIdentifier")   == F.col("rd.HerdIdentifier")),
    "left"
)

total_d   = sensor_digits.count()
matched_d = digits_match.where(F.col("rd.LifeNumberDigits").isNotNull()).count()
truly_absent = digits_match.where(F.col("rd.LifeNumberDigits").isNull()).count()

print(f"Unmatched sensor animals total      : {total_d:,}")
print(f"Would match on digits only          : {matched_d:,}  ({matched_d/total_d*100:.1f}%)")
print(f"Truly absent from ReferenceData     : {truly_absent:,}  ({truly_absent/total_d*100:.1f}%)")
print()
print("Sample of truly absent animals:")
display(digits_match
    .where(F.col("rd.LifeNumberDigits").isNull())
    .select(
        F.col("sd.HerdIdentifier"),
        F.col("sd.SensorLifeNumber"),
        F.col("sd.SensorDigitsOnly")
    )
    .distinct()
    .limit(20))

print("\nAbsent animals per farm:")
display(digits_match
    .where(F.col("rd.LifeNumberDigits").isNull())
    .groupBy(F.col("sd.HerdIdentifier"))
    .agg(F.countDistinct("sd.SensorLifeNumber").alias("AbsentAnimals"))
    .orderBy("sd.HerdIdentifier"))

# COMMAND ----------

# Some rows in SensorData may not have LifeNumber in AttributeMap at all.
# Check whether AnimalIdentifier alone (+ HerdIdentifier) gives good coverage.

sensor_animals = (silver_sensor
    .select("HerdIdentifier", "AnimalIdentifier")
    .distinct()
    .alias("sa"))

ref_animals = (silver_ref
    .select("HerdIdentifier", "AnimalIdentifier")
    .distinct()
    .alias("ra"))

matched_a = sensor_animals.join(
    ref_animals,
    (F.col("sa.HerdIdentifier")   == F.col("ra.HerdIdentifier")) &
    (F.col("sa.AnimalIdentifier") == F.col("ra.AnimalIdentifier")),
    "inner"
).count()

total_a   = sensor_animals.count()
unmatched_a = total_a - matched_a

print(f"Distinct animals in SensorData             : {total_a:,}")
print(f"Matched to ReferenceData (HerdId+AnimalId) : {matched_a:,}  ({matched_a/total_a*100:.1f}%)")
print(f"Unmatched                                  : {unmatched_a:,}  ({unmatched_a/total_a*100:.1f}%)")
print()
print("Animals in SensorData not in ReferenceData (sample):")
display(sensor_animals.join(
    ref_animals,
    (F.col("sa.HerdIdentifier")   == F.col("ra.HerdIdentifier")) &
    (F.col("sa.AnimalIdentifier") == F.col("ra.AnimalIdentifier")),
    "left_anti"
).select(
    F.col("sa.HerdIdentifier"),
    F.col("sa.AnimalIdentifier")
).limit(20))

# COMMAND ----------

# Does THI cover the same farms and date range as the sensor data and reference data?

print("THI date range per farm:")
display(silver_thi
    .groupBy("HerdIdentifier")
    .agg(
        F.min("MeasurementDate").alias("EarliestDate"),
        F.max("MeasurementDate").alias("LatestDate"),
        F.count("*").alias("HourlyReadings")
    )
    .orderBy("HerdIdentifier"))

print("\nSensorData EventDate range per farm:")
display(silver_sensor
    .groupBy("HerdIdentifier")
    .agg(
        F.min(F.to_date("EventDate")).alias("EarliestDate"),
        F.max(F.to_date("EventDate")).alias("LatestDate"),
        F.count("*").alias("SensorRows")
    )
    .orderBy("HerdIdentifier"))

print("\nReferenceData CalvingDate range per farm:")
display(silver_ref
    .groupBy("HerdIdentifier")
    .agg(
        F.min("CalvingDate").alias("EarliestCalving"),
        F.max("CalvingDate").alias("LatestCalving"),
        F.count("*").alias("CowLactations")
    )
    .orderBy("HerdIdentifier"))

# COMMAND ----------

# Build a preview of the lactation window table.
# One row per cow-lactation with start and end dates.
# LactationEnd = next CalvingDate for same animal, or CalvingDate + 365 for last lactation.
# This is what lets us tag sensor readings with DIM and Parity.

from pyspark.sql import Window

w = Window.partitionBy("HerdIdentifier", "AnimalIdentifier").orderBy("CalvingDate")

lactation_windows = (silver_ref
    .where(F.col("CalvingDate").isNotNull())
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        "LifeNumber",
        "LifeNumberDigits",
        "Parity",
        "CalvingDate"
    )
    .withColumn("LactationStart", F.col("CalvingDate"))
    .withColumn("LactationEnd",
        F.coalesce(
            F.lead("CalvingDate", 1).over(w),
            F.date_add(F.col("CalvingDate"), 365)
        ))
    .drop("CalvingDate")
)

print(f"Lactation windows: {lactation_windows.count():,}")
print("Sample:")
display(lactation_windows.orderBy("HerdIdentifier", "AnimalIdentifier", "LactationStart").limit(20))

# COMMAND ----------

print("=" * 55)
print("  Gold Layer Readiness Check")
print("=" * 55)
print()
print("Run this after all cells above and fill in manually:")
print()
print("  HerdIdentifier consistent across 3 datasets : [YES/NO]")
print("  Exact LifeNumber match coverage             : [X%]")
print("  Digits-only match coverage                  : [X%]")
print("  AnimalIdentifier match coverage             : [X%]")
print("  Best join key identified                    : [LifeNumber/AnimalIdentifier/both]")
print("  THI covers full sensor date range           : [YES/NO/PARTIAL]")
print("  Lactation window table looks correct        : [YES/NO]")
print()
print("If AnimalIdentifier coverage >= 90% -> proceed with gold build")
print("If coverage < 90% -> investigate unmatched animals before proceeding")
print("=" * 55)