# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 10 — DailyMilkYield
# MAGIC One row per cow per day of milk recording, from silver `MilkYield`.
# MAGIC
# MAGIC * **Cow key:** `LifeNumberDigits` (pseudonymised) — the same value as `BestLifeNumberDigits` in GoldMaster and
# MAGIC   as `CowID` in the SOS24h/SOS2h exports. Rows without a life number cannot be linked and are left out.
# MAGIC * **Repeated records for one cow-day** (the milking system sometimes stores a day twice with near-identical
# MAGIC   yields) are averaged, not summed. If they disagree on the calving date, the latest calving on or before the
# MAGIC   day is used.
# MAGIC * **0 kg** and fat/protein values outside the Rulebook range are treated as missing.
# MAGIC * `HasSensorData` marks cow-days that also exist in GoldMaster (sensor data), so they can be joined.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG       = "senseofsensors_catalog"
SILVER_SCHEMA = f"{CATALOG}.silver"
SILVER_VOLUME = f"/Volumes/{CATALOG}/silver/silver_volume"
GOLD_VOLUME   = f"/Volumes/{CATALOG}/gold/gold_volume"
SRC_PATH      = f"{SILVER_VOLUME}/MilkYield"
GM_PATH       = f"{GOLD_VOLUME}/GoldMaster"
OUT_PATH      = f"{GOLD_VOLUME}/DailyMilkYield"

milk = spark.read.format("delta").load(SRC_PATH)
gm   = spark.read.format("delta").load(GM_PATH)

ranges = {r.TargetColumn: (r.MinValue, r.MaxValue) for r in
          spark.table(f"{SILVER_SCHEMA}.Rulebook").where("Dataset = 'MilkYield'").collect()
          if r.TargetColumn in ("MilkFatPct", "MilkProteinPct")}
print(f"Silver MilkYield rows: {milk.count():,} · content ranges used: {ranges}")

# COMMAND ----------

n_all = milk.count()
m = milk.where(F.col("LifeNumberDigits").isNotNull() & F.col("EventDay").isNotNull())
print(f"Left out (no life number or no date): {n_all - m.count():,}")

# Values treated as missing
m = m.withColumn("_zero", F.col("DailyMilkKg") == 0) \
     .withColumn("DailyMilkKg", F.when(F.col("DailyMilkKg") > 0, F.col("DailyMilkKg")))
for c, (lo, hi) in ranges.items():
    m = m.withColumn(c, F.when(F.col(c).between(lo, hi), F.col(c)))

# One lactation per cow-day: latest calving on or before the day
lact_rank = Window.partitionBy("HerdIdentifier", "LifeNumberDigits", "EventDay").orderBy(
    (F.col("CalvingDate") <= F.col("EventDay")).desc(), F.col("CalvingDate").desc())
m = (m.withColumn("_lr", F.dense_rank().over(lact_rank))
      .withColumn("_conflict", F.max("_lr").over(Window.partitionBy("HerdIdentifier", "LifeNumberDigits", "EventDay")) > 1)
      .where("_lr = 1"))

daily = (m.groupBy("HerdIdentifier", "LifeNumberDigits", "EventDay")
          .agg(F.first("AnimalIdentifier", ignorenulls=True).alias("AnimalIdentifier"),
               F.first("LactationNumber", ignorenulls=True).alias("LactationNumber"),
               F.first("CalvingDate", ignorenulls=True).alias("CalvingDate"),
               F.first("PreviousDryOffDate", ignorenulls=True).alias("PreviousDryOffDate"),
               F.first("DryPeriodDays", ignorenulls=True).alias("DryPeriodDays"),
               F.round(F.avg("DailyMilkKg"), 2).alias("DailyMilkKg"),
               F.round(F.avg("MilkFatPct"), 2).alias("MilkFatPct"),
               F.round(F.avg("MilkProteinPct"), 2).alias("MilkProteinPct"),
               F.count("*").alias("MilkRecords"),
               F.max(F.col("_zero").cast("int")).cast("boolean").alias("ZeroMilkRecorded"),
               F.max(F.col("_conflict").cast("int")).cast("boolean").alias("CalvingDateConflict"))
          .withColumn("DaysInMilk", F.datediff("EventDay", "CalvingDate"))
          .withColumn("RepeatedDayRecord", F.col("MilkRecords") > 1))

# Link to the sensor data: does GoldMaster have a row for this cow on this day?
sensor_days = (gm.where(F.col("BestLifeNumberDigits").isNotNull())
                 .groupBy("HerdIdentifier", F.col("BestLifeNumberDigits").alias("LifeNumberDigits"), "EventDay")
                 .agg(F.first("LactationStart", ignorenulls=True).alias("_gmLactationStart"))
                 .withColumn("_in_gm", F.lit(True)))
daily = (daily.join(sensor_days, ["HerdIdentifier", "LifeNumberDigits", "EventDay"], "left")
              .withColumn("HasSensorData", F.coalesce("_in_gm", F.lit(False)))
              .withColumn("CalvingDateMatchesGold", F.when(F.col("_gmLactationStart").isNotNull(),
                                                           F.col("_gmLactationStart") == F.col("CalvingDate"))))

ORDER = ["HerdIdentifier", "LifeNumberDigits", "AnimalIdentifier", "EventDay", "LactationNumber", "CalvingDate",
         "DaysInMilk", "PreviousDryOffDate", "DryPeriodDays", "DailyMilkKg", "MilkFatPct", "MilkProteinPct",
         "MilkRecords", "RepeatedDayRecord", "ZeroMilkRecorded", "CalvingDateConflict", "HasSensorData",
         "CalvingDateMatchesGold"]
daily = daily.select(*ORDER)

# COMMAND ----------

(daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
      .partitionBy("HerdIdentifier").save(OUT_PATH))
out = spark.read.format("delta").load(OUT_PATH)
print(f"WROTE {out.count():,} cow-days -> {OUT_PATH}")

# COMMAND ----------

display(out.groupBy("HerdIdentifier").agg(
    F.count("*").alias("CowDays"),
    F.countDistinct("LifeNumberDigits").alias("Cows"),
    F.min("EventDay").alias("FirstDay"), F.max("EventDay").alias("LastDay"),
    F.round(F.avg("DailyMilkKg"), 1).alias("MeanMilkKg"),
    F.round(F.avg(F.col("HasSensorData").cast("int")) * 100, 1).alias("PctWithSensorData"),
    F.round(F.avg(F.col("CalvingDateMatchesGold").cast("int")) * 100, 1).alias("PctCalvingMatchesGold"),
    F.sum(F.col("RepeatedDayRecord").cast("int")).alias("RepeatedDays"),
    F.sum(F.col("CalvingDateConflict").cast("int")).alias("CalvingConflicts")).orderBy("HerdIdentifier"))

# Lactation curve check: mean daily milk by days in milk (should rise to a peak around 40–60 days)
display(out.where(F.col("DaysInMilk").between(0, 105)).groupBy("HerdIdentifier", "DaysInMilk")
           .agg(F.round(F.avg("DailyMilkKg"), 1).alias("MeanMilkKg")).orderBy("HerdIdentifier", "DaysInMilk"))
