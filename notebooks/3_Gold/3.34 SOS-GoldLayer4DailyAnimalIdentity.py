# Databricks notebook source
#Gold DailyAnimalIdentity (reused cow numbers & moved sensors)

# Cell 1 — Setup
from pyspark.sql import functions as F
from pyspark.sql import Window as W

CATALOG        = "senseofsensors_catalog"
SILVER_VOLUME  = "/Volumes/senseofsensors_catalog/silver/silver_volume"
GOLD_VOLUME    = "/Volumes/senseofsensors_catalog/gold/gold_volume"

SENSOR_PATH    = f"{SILVER_VOLUME}/SensorData"
CALENDAR_TABLE = f"{CATALOG}.silver.calendardata"
DBS_PATH       = f"{GOLD_VOLUME}/DailyBehaviourSummary"
LW_PATH        = f"{GOLD_VOLUME}/LactationWindows"
EVIDENCE_PATH  = f"{GOLD_VOLUME}/_work/SensorDailyIdentityEvidence"
OUT_PATH       = f"{GOLD_VOLUME}/DailyAnimalIdentity"

def digits(c):
    """Life number -> digits only ('NL 123456789' -> '123456789'); empty -> null."""
    d = F.regexp_replace(F.col(c) if isinstance(c, str) else c, r"\D", "")
    return F.when(d == "", None).otherwise(d)

def herd_key(c="HerdIdentifier"):
    return F.trim(F.col(c).cast("string"))

def animal_key(c="AnimalIdentifier"):
    # cast to string, trim, strip leading zeros so '0512' and 512 join
    return F.regexp_replace(F.trim(F.col(c).cast("string")), r"^0+(?=\d)", "")

dbs = spark.read.format("delta").load(DBS_PATH)
lw  = spark.read.format("delta").load(LW_PATH)
cal = spark.table(CALENDAR_TABLE)

days = (dbs.select("HerdIdentifier", "AnimalIdentifier", "EventDay")
           .withColumn("HerdKey", herd_key())
           .withColumn("AnimalKey", animal_key()))

DBS_ROWS = days.count()
print(f"DailyBehaviourSummary days : {DBS_ROWS:,}  (expected 2,530,267)")

# COMMAND ----------

# Cell 2 — Sensor evidence per cow number per day (one pass over SensorData, written once)
# LifeNumber and Responder come from the SensorData AttributeMap (PascalCase keys from silver).
# ~1.2B rows are scanned once; the result is <= 2.5M rows and is saved so it is never recomputed.

evidence = (spark.read.format("delta").load(SENSOR_PATH)
    .select(
        herd_key().alias("HerdKey"),
        animal_key().alias("AnimalKey"),
        F.to_date("EventDate").alias("EventDay"),
        digits(F.col("AttributeMap").getItem("LifeNumber")).alias("SensorLN"),
        F.trim(F.col("AttributeMap").getItem("Responder")).alias("Responder"))
    .where(F.col("SensorLN").isNotNull() | F.col("Responder").isNotNull())
    .groupBy("HerdKey", "AnimalKey", "EventDay")
    .agg(F.array_sort(F.collect_set("SensorLN")).alias("SensorLifeNumbers"),
         F.array_sort(F.collect_set("Responder")).alias("Responders")))

evidence.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(EVIDENCE_PATH)
evidence = spark.read.format("delta").load(EVIDENCE_PATH)

print(f"Cow-number-days with sensor identity evidence : {evidence.count():,}")
print(f"  ...with exactly 1 LifeNumber                : {evidence.where(F.size('SensorLifeNumbers') == 1).count():,}")
print(f"  ...with 2+ LifeNumbers (handover days)      : {evidence.where(F.size('SensorLifeNumbers') > 1).count():,}")

# COMMAND ----------

# Cell 3 — Calvings per cow number (CalendarData + ReferenceData) and last-seen dates per cow

cal_ln = F.coalesce(F.col("AttributeMap").getItem("Life number"),
                    F.col("AttributeMap").getItem("Lifenumber2"))

cal_events = (cal
    .select(herd_key().alias("HerdKey"),
            animal_key().alias("AnimalKey"),
            F.to_date("EventDate").alias("EvDay"),
            "ObservationType",
            digits(cal_ln).alias("LifeNumberDigits"))
    .where(F.col("LifeNumberDigits").isNotNull() & F.col("EvDay").isNotNull()))

calv_cal = (cal_events.where(F.col("ObservationType") == "CalvingEvent")
    .select("HerdKey", "AnimalKey", F.col("EvDay").alias("CalvingDay"), "LifeNumberDigits",
            F.lit(2).alias("SrcRank")))

calv_ref = (lw
    .select(herd_key().alias("HerdKey"),
            animal_key().alias("AnimalKey"),
            F.to_date("LactationStart").alias("CalvingDay"),
            F.col("LifeNumberDigits").cast("string").alias("LifeNumberDigits"),
            F.lit(1).alias("SrcRank"))
    .where(F.col("LifeNumberDigits").isNotNull() & F.col("CalvingDay").isNotNull()))

# one calving per cow number per day; ReferenceData wins if the two sources disagree
w_calv = W.partitionBy("HerdKey", "AnimalKey", "CalvingDay").orderBy("SrcRank")
calvings = (calv_ref.unionByName(calv_cal)
    .withColumn("r", F.row_number().over(w_calv))
    .where("r = 1").drop("r", "SrcRank")
    .cache())

# last/first date each cow is seen under each cow number (calendar events + calvings + single-LN sensor days)
sensor_seen = (evidence.where(F.size("SensorLifeNumbers") == 1)
    .select("HerdKey", "AnimalKey", F.col("EventDay").alias("EvDay"),
            F.col("SensorLifeNumbers")[0].alias("LifeNumberDigits")))

seen = (cal_events.select("HerdKey", "AnimalKey", "EvDay", "LifeNumberDigits")
    .unionByName(calvings.select("HerdKey", "AnimalKey", F.col("CalvingDay").alias("EvDay"), "LifeNumberDigits"))
    .unionByName(sensor_seen)
    .groupBy("HerdKey", "AnimalKey", "LifeNumberDigits")
    .agg(F.min("EvDay").alias("FirstSeen"), F.max("EvDay").alias("LastSeen"))
    .cache())

# cow numbers that were used by more than one cow
reused = (seen.groupBy("HerdKey", "AnimalKey")
    .agg(F.countDistinct("LifeNumberDigits").alias("CowsOnThisNumber")))

# format sanity: do calendar life numbers match ReferenceData life numbers?
lw_ln = lw.select(herd_key().alias("HerdKey"), F.col("LifeNumberDigits").cast("string").alias("LifeNumberDigits")).distinct()
cal_ln_set = calv_cal.select("HerdKey", "LifeNumberDigits").distinct()
match = cal_ln_set.join(lw_ln, ["HerdKey", "LifeNumberDigits"], "inner").count()
sen_ln_set = sensor_seen.select("HerdKey", "LifeNumberDigits").distinct()
sen_match = sen_ln_set.join(lw_ln, ["HerdKey", "LifeNumberDigits"], "inner").count()

print(f"Calvings (merged, one per cow number per day) : {calvings.count():,}")
print(f"Calendar life numbers found in LactationWindows: {match:,} of {cal_ln_set.count():,}")
print(f"Sensor life numbers found in LactationWindows  : {sen_match:,} of {sen_ln_set.count():,}")
print("Cow numbers used by more than one cow, per farm:")
display(reused.groupBy("HerdKey")
        .agg(F.count("*").alias("CowNumbers"),
             F.sum(F.when(F.col("CowsOnThisNumber") > 1, 1).otherwise(0)).alias("ReusedCowNumbers"),
             F.max("CowsOnThisNumber").alias("MaxCowsOnOneNumber"))
        .orderBy("HerdKey"))

# COMMAND ----------

# Cell 4 — Assign every sensor day to one cow

# 4a. previous and next calving for each day (struct max/min = latest/earliest calving)
prev_next = (days.select("HerdKey", "AnimalKey", "EventDay")
    .join(calvings, ["HerdKey", "AnimalKey"], "inner")
    .groupBy("HerdKey", "AnimalKey", "EventDay")
    .agg(F.max(F.when(F.col("CalvingDay") <= F.col("EventDay"),
                      F.struct("CalvingDay", "LifeNumberDigits"))).alias("prev"),
         F.min(F.when(F.col("CalvingDay") >  F.col("EventDay"),
                      F.struct("CalvingDay", "LifeNumberDigits"))).alias("next")))

prev_last = seen.select("HerdKey", "AnimalKey",
                        F.col("LifeNumberDigits").alias("PrevLN"),
                        F.col("LastSeen").alias("PrevLastSeen"))

a = (days
    .join(prev_next, ["HerdKey", "AnimalKey", "EventDay"], "left")
    .withColumn("PrevLN", F.col("prev.LifeNumberDigits"))
    .withColumn("NextLN", F.col("next.LifeNumberDigits"))
    .join(prev_last, ["HerdKey", "AnimalKey", "PrevLN"], "left")
    .join(evidence, ["HerdKey", "AnimalKey", "EventDay"], "left")
    .join(reused, ["HerdKey", "AnimalKey"], "left"))

# 4b. sensor evidence: that day, or filled from the nearest days on both sides when they agree
a = a.withColumn("SensorLN", F.when(F.size("SensorLifeNumbers") == 1, F.col("SensorLifeNumbers")[0]))
w_back = W.partitionBy("HerdKey", "AnimalKey").orderBy("EventDay").rowsBetween(W.unboundedPreceding, 0)
w_fwd  = W.partitionBy("HerdKey", "AnimalKey").orderBy("EventDay").rowsBetween(0, W.unboundedFollowing)
a = (a.withColumn("LN_before", F.last("SensorLN", ignorenulls=True).over(w_back))
      .withColumn("LN_after",  F.first("SensorLN", ignorenulls=True).over(w_fwd)))

# 4c. original calving rule (with the last-seen refinement for reused numbers)
p, n = F.col("prev"), F.col("next")
rule_ln = (F.when(p.isNull() & n.isNull(), None)
            .when(p.isNull(), F.col("NextLN"))
            .when(n.isNull(), F.col("PrevLN"))
            .when(F.col("PrevLN") == F.col("NextLN"), F.col("PrevLN"))
            .when(F.col("EventDay") <= F.col("PrevLastSeen"), F.col("PrevLN"))
            .otherwise(F.col("NextLN")))
rule_method = (F.when(p.isNull() & n.isNull(), "NoCalvingRecord")
                .when(p.isNull(), "Calving_BeforeFirstCalving")
                .when(n.isNull(), "Calving_AfterLastCalving")
                .when(F.col("PrevLN") == F.col("NextLN"), "Calving_SameCow")
                .when(F.col("EventDay") <= F.col("PrevLastSeen"), "Calving_ReusedNumber_PreviousCow")
                .otherwise("Calving_ReusedNumber_NextCow"))
a = a.withColumn("RuleLN", rule_ln).withColumn("RuleMethod", rule_method)

# 4d. final choice: direct sensor evidence > filled sensor evidence > calving rule
filled_ok = F.col("LN_before").isNotNull() & (F.col("LN_before") == F.col("LN_after"))
handover  = F.coalesce(F.size("SensorLifeNumbers"), F.lit(0)) > 1

identity = (a
    .withColumn("BestLifeNumberDigits",
        F.when(F.col("SensorLN").isNotNull(), F.col("SensorLN"))
         .when(~handover & filled_ok, F.col("LN_before"))
         .otherwise(F.col("RuleLN")))
    .withColumn("IdentityMethod",
        F.when(F.col("SensorLN").isNotNull(), F.lit("SensorLifeNumber"))
         .when(~handover & filled_ok, F.lit("SensorLifeNumberFilled"))
         .when(handover, F.concat(F.lit("SensorHandoverDay_"), F.col("RuleMethod")))
         .otherwise(F.col("RuleMethod")))
    .withColumn("IdentityConflict",
        F.col("RuleLN").isNotNull() & F.col("BestLifeNumberDigits").isNotNull() &
        (F.col("RuleLN") != F.col("BestLifeNumberDigits")))
    .withColumn("CowNumberReused", F.coalesce(F.col("CowsOnThisNumber"), F.lit(1)) > 1)
    .withColumn("ResponderCount", F.when(F.col("Responders").isNull(), 0).otherwise(F.size("Responders"))))

# HasRefData = this cow exists in ReferenceData (LactationWindows) on this farm
identity = (identity
    .join(lw_ln.withColumnRenamed("LifeNumberDigits", "BestLifeNumberDigits").withColumn("HasRefData", F.lit(True)),
          ["HerdKey", "BestLifeNumberDigits"], "left")
    .withColumn("HasRefData", F.coalesce(F.col("HasRefData"), F.lit(False))))

daily_identity = identity.select(
    "HerdIdentifier", "AnimalIdentifier", "EventDay",
    "BestLifeNumberDigits", "IdentityMethod", "HasRefData",
    "CowNumberReused", "IdentityConflict",
    F.col("SensorLifeNumbers"), F.col("Responders"), "ResponderCount",
    F.col("prev.CalvingDay").alias("PrevCalvingDay"),
    F.col("next.CalvingDay").alias("NextCalvingDay"))

# COMMAND ----------

# Cell 5 — Checks, impact versus the old bridge, write

out_rows = daily_identity.count()
dupes = daily_identity.groupBy("HerdIdentifier", "AnimalIdentifier", "EventDay").count().where("count > 1").count()
print(f"Rows   : {out_rows:,}  (must equal {DBS_ROWS:,})  {'ok' if out_rows == DBS_ROWS else ' STOP'}")
print(f"Dupes  : {dupes}  {'ok' if dupes == 0 else ' STOP'}")

print("\nHow each day's cow was decided:")
display(daily_identity.groupBy("IdentityMethod").count()
        .withColumn("Pct", F.round(F.col("count") * 100 / F.lit(out_rows), 2))
        .orderBy(F.desc("count")))

# impact: compare with the old one-LifeNumber-per-cow-number bridge used by Gold 7
old = spark.read.format("delta").load(f"{GOLD_VOLUME}/AnimalIdentityBridge")
w_old = W.partitionBy("HerdIdentifier", "SensorAnimalIdentifier").orderBy(
    F.when(F.col("JoinMethod") == "LifeNumber", 1).when(F.col("JoinMethod") == "AnimalIdentifier", 2).otherwise(3))
old_slim = (old.withColumn("r", F.row_number().over(w_old)).where("r = 1")
    .select("HerdIdentifier", F.col("SensorAnimalIdentifier").alias("AnimalIdentifier"),
            F.col("BestLifeNumberDigits").cast("string").alias("OldLN")))
cmp = daily_identity.join(old_slim, ["HerdIdentifier", "AnimalIdentifier"], "left")
changed = cmp.where(~F.col("BestLifeNumberDigits").eqNullSafe(F.col("OldLN"))).count()

print(f"Days on a reused cow number              : {daily_identity.where('CowNumberReused').count():,}")
print(f"Days where the cow differs from old Gold 7: {changed:,}  ({changed*100/out_rows:.2f}%)")
print(f"Days where sensor evidence overrules rule : {daily_identity.where('IdentityConflict').count():,}")
print(f"Days with no identity at all              : {daily_identity.where(F.col('BestLifeNumberDigits').isNull()).count():,}")

# moved sensors: one Responder seen on more than one cow number
resp = (evidence.select("HerdKey", "AnimalKey", F.explode("Responders").alias("Responder"))
        .groupBy("HerdKey", "Responder").agg(F.countDistinct("AnimalKey").alias("CowNumbers")))
print(f"Sensors (Responders) worn under >1 cow number: {resp.where('CowNumbers > 1').count():,} of {resp.count():,}")
print("  -> these are fine: sensor data follows the cow number, so a moved sensor's readings land on the new cow.")

daily_identity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(OUT_PATH)
print(f"\nWritten: {OUT_PATH}")

# COMMAND ----------

from pyspark.sql import functions as F

dai = spark.read.format("delta").load("/Volumes/senseofsensors_catalog/gold/gold_volume/DailyAnimalIdentity")
total = dai.count()
has_ref = dai.where(F.col("HasRefData") == True).count()
no_identity = dai.where(F.col("BestLifeNumberDigits").isNull()).count()

print(f"Total animal-days        : {total:,}")
print(f"HasRefData = True        : {has_ref:,}  ({has_ref*100/total:.1f}%)")
print(f"No identity at all       : {no_identity:,}  ({no_identity*100/total:.1f}%)")

display(dai.where(F.col("BestLifeNumberDigits").isNull())
    .groupBy("HerdIdentifier")
    .count()
    .withColumn("Pct", F.round(F.col("count") * 100 / total, 1))
    .orderBy("HerdIdentifier"))

# COMMAND ----------

evidence_check = spark.read.format("delta").load("/Volumes/senseofsensors_catalog/gold/gold_volume/_work/SensorDailyIdentityEvidence")
display(evidence_check.select("HerdKey").distinct().orderBy("HerdKey"))

# COMMAND ----------

from pyspark.sql import functions as F

dbs = spark.read.format("delta").load("/Volumes/senseofsensors_catalog/gold/gold_volume/DailyBehaviourSummary")
lw  = spark.read.format("delta").load("/Volumes/senseofsensors_catalog/gold/gold_volume/LactationWindows")

print("DailyBehaviourSummary distinct HerdIdentifier:")
display(dbs.select("HerdIdentifier").distinct().orderBy("HerdIdentifier"))

print("LactationWindows distinct HerdIdentifier:")
display(lw.select("HerdIdentifier").distinct().orderBy("HerdIdentifier"))

# COMMAND ----------

bridge = spark.read.format("delta").load("/Volumes/senseofsensors_catalog/gold/gold_volume/AnimalIdentityBridge")
display(bridge.select("HerdIdentifier").distinct().orderBy("HerdIdentifier"))

# COMMAND ----------

# Verify output is clean
dai = spark.read.format("delta").load("/Volumes/senseofsensors_catalog/gold/gold_volume/DailyAnimalIdentity")

print("Distinct HerdIdentifiers:")
dai.select("HerdIdentifier").distinct().orderBy("HerdIdentifier").show()

print(f"Total rows: {dai.count():,}  (must equal 2,530,267)")