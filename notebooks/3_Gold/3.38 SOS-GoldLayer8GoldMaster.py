# Databricks notebook source
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG       = dbutils.widgets.get("catalog")
SILVER_VOLUME = dbutils.widgets.get("silver_volume").rstrip("/")
GOLD_SCHEMA   = f"{CATALOG}.gold"

OUT_TABLE = f"{GOLD_SCHEMA}.GoldMaster"

print("Writing to :", OUT_TABLE)

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"

lw   = spark.read.format("delta").load(f"{GOLD_VOLUME}/LactationWindows")
cf   = spark.read.format("delta").load(f"{GOLD_VOLUME}/ClinicalFlags")
rt   = spark.read.format("delta").load(f"{GOLD_VOLUME}/ReproductiveTimeline")
dbs  = spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyBehaviourSummary")
thi  = spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyTHISummary")
aib  = spark.read.format("delta").load(f"{GOLD_VOLUME}/AnimalIdentityBridge")

print(f"LactationWindows      : {lw.count():>10,}")
print(f"ClinicalFlags         : {cf.count():>10,}")
print(f"ReproductiveTimeline  : {rt.count():>10,}")
print(f"DailyBehaviourSummary : {dbs.count():>10,}")
print(f"DailyTHISummary       : {thi.count():>10,}")
print(f"AnimalIdentityBridge  : {aib.count():>10,}")

# COMMAND ----------

from pyspark.sql import Window as W

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"

dai = (spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyAnimalIdentity")
    .select("HerdIdentifier", "AnimalIdentifier", "EventDay",
            "BestLifeNumberDigits", "HasRefData",
            F.col("IdentityMethod").alias("JoinMethod"),
            "CowNumberReused", "IdentityConflict"))

spine = dbs.join(dai, ["HerdIdentifier", "AnimalIdentifier", "EventDay"], "left")
print(f"Spine rows (should be 2,530,267): {spine.count():,}")

# COMMAND ----------

#lactation window join
lw_slim = (lw
    .select(
        F.col("HerdIdentifier").alias("lw_HerdIdentifier"),
        F.col("LifeNumberDigits").alias("lw_LifeNumberDigits"),
        F.col("AnimalIdentifier").alias("RefAnimalIdentifier"),
        "Parity", "LactationStart", "LactationEnd",
        "LactationLengthDays", "LactationEndSource",
        "CalvingType", "CalvingTypeLabel", "CalvingDurationHr",
        "Afterbirth", "AfterbirthLabel",
        "BcsStartDryPeriod", "BcsEndDryPeriod",
        "LocoStartDryPeriod", "LocoEndDryPeriod",
        "CalciumLevel", "BHBA1", "BHBA2",
        "CycleWeek4", "CycleWeek8",
        "Repro4UterusLabel", "Repro8UterusLabel",
        "SubclinicalKetosis", "SubclinicalHypocalcaemia",
        "DifficultCalving", "RetainedPlacenta", "BcsChangeDryPeriod"
    ))

gold_master = (spine
    .join(
        lw_slim,
        (F.col("HerdIdentifier")       == F.col("lw_HerdIdentifier")) &
        (F.col("BestLifeNumberDigits") == F.col("lw_LifeNumberDigits")) &
        (F.col("EventDay")             >= F.date_add(F.col("LactationStart"), -21)) &
        (F.col("EventDay")             <= F.col("LactationEnd")),
        "left"
    )
    .drop("lw_HerdIdentifier", "lw_LifeNumberDigits"))

print(f"After LactationWindows join : {gold_master.count():,}")

# COMMAND ----------

gold_master = (gold_master
    # DaysInMilk — positive = days after calving, negative = days before calving
    .withColumn("DaysInMilk",
        F.when(F.col("LactationStart").isNotNull(),
               F.datediff(F.col("EventDay"), F.col("LactationStart")))
    )

    # LactationPhase — standard dairy research periods
    .withColumn("LactationPhase",
        F.when(F.col("DaysInMilk").isNull(),           None)
         .when(F.col("DaysInMilk") < 0,                F.lit("PreCalving"))
         .when(F.col("DaysInMilk") <= 21,              F.lit("EarlyTransition"))
         .when(F.col("DaysInMilk") <= 60,              F.lit("EarlyLactation"))
         .when(F.col("DaysInMilk") <= 150,             F.lit("MidLactation"))
         .when(F.col("DaysInMilk") <= 305,             F.lit("LateLactation"))
         .otherwise(                                    F.lit("Extended"))
    )

    # PreCalvingWeek — week before calving for transition analysis
    .withColumn("PreCalvingWeek",
        F.when(F.col("DaysInMilk").between(-7,   -1),  F.lit("Week1Before"))
         .when(F.col("DaysInMilk").between(-14,  -8),  F.lit("Week2Before"))
         .when(F.col("DaysInMilk").between(-21, -15),  F.lit("Week3Before"))
         .when(F.col("DaysInMilk").between(-28, -22),  F.lit("Week4Before"))
         .otherwise(None)
    )

    # CalvingSeason — quarter of year at calving
    .withColumn("CalvingSeason",
        F.when(F.col("LactationStart").isNull(), None)
         .when(F.month("LactationStart").isin([12, 1, 2]),  F.lit("Winter"))
         .when(F.month("LactationStart").isin([3, 4, 5]),   F.lit("Spring"))
         .when(F.month("LactationStart").isin([6, 7, 8]),   F.lit("Summer"))
         .otherwise(                                         F.lit("Autumn"))
    )
)

print("DaysInMilk, LactationPhase, PreCalvingWeek, CalvingSeason added")

# COMMAND ----------

#clinical flags join
cf_slim = (cf
    .select(
        F.col("HerdIdentifier").alias("cf_HerdIdentifier"),
        F.col("AnimalIdentifier").alias("cf_AnimalIdentifier"),
        F.col("LactationStart").alias("cf_LactationStart"),
        *[c for c in cf.columns if c.startswith("Note")]
    ))

gold_master = gold_master.select([F.col(c).alias(c) for c in gold_master.columns])

gold_master = (gold_master
    .join(
        cf_slim,
        (F.col("HerdIdentifier")      == F.col("cf_HerdIdentifier")) &
        (F.col("RefAnimalIdentifier") == F.col("cf_AnimalIdentifier")) &
        (F.col("LactationStart")      == F.col("cf_LactationStart")),
        "left"
    )
    .drop("cf_HerdIdentifier", "cf_AnimalIdentifier", "cf_LactationStart"))

print(f"After ClinicalFlags join : {gold_master.count():,}")

# COMMAND ----------

#repro timeline
rt_slim = (rt
    .select(
        F.col("HerdIdentifier").alias("rt_HerdIdentifier"),
        F.col("AnimalIdentifier").alias("rt_AnimalIdentifier"),
        F.col("LactationStart").alias("rt_LactationStart"),
        "FirstInseminationDate", "LastInseminationDate",
        "InseminationCount", "DimFirstInsemination", "DimLastInsemination",
        "FirstHeatDate", "HeatEventCount", "DimFirstHeat",
        "FirstPregnancyCheckDate", "PregnancyCheckCount", "DimFirstPregnancyCheck",
        "KeepOpenCount", "DryoffDate", "DimDryoff",
        "NextCalvingDate", "CalvingIntervalDays", "DaysOpen",
        "ConceptionAtFirstInsemination", "ReproductiveSuccess", "EarlyFirstInsemination"
    ))

gold_master = gold_master.select([F.col(c).alias(c) for c in gold_master.columns])

gold_master = (gold_master
    .join(
        rt_slim,
        (F.col("HerdIdentifier")      == F.col("rt_HerdIdentifier")) &
        (F.col("RefAnimalIdentifier") == F.col("rt_AnimalIdentifier")) &
        (F.col("LactationStart")      == F.col("rt_LactationStart")),
        "left"
    )
    .drop("rt_HerdIdentifier", "rt_AnimalIdentifier", "rt_LactationStart"))

print(f"After ReproductiveTimeline join : {gold_master.count():,}")

# COMMAND ----------

#thi join
thi_slim = (thi
    .select(
        F.col("HerdIdentifier").alias("thi_HerdIdentifier"),
        F.col("MeasurementDate").alias("thi_EventDay"),
        "MaxTHIExternal", "MeanTHIExternal",
        "HeatStressHoursExternal", "SevereHeatStressHoursExternal",
        "CumulativeHeatLoadExternal",
        "MaxTHIInternal", "MeanTHIInternal", "HeatStressHoursInternal",
        "DailyTHIStatusExternal", "DailyTHIStatusInternal"
    ))

gold_master = gold_master.select([F.col(c).alias(c) for c in gold_master.columns])

gold_master = (gold_master
    .join(
        thi_slim,
        (F.col("HerdIdentifier") == F.col("thi_HerdIdentifier")) &
        (F.col("EventDay")       == F.col("thi_EventDay")),
        "left"
    )
    .drop("thi_HerdIdentifier", "thi_EventDay"))

print(f"After THI join : {gold_master.count():,}")

# COMMAND ----------

# Select and order columns logically for research use
gold_master = gold_master.select(
    # Identity
    "HerdIdentifier",
    "AnimalIdentifier",
    "BestLifeNumberDigits",
    "RefAnimalIdentifier",
    "HasRefData",
    "JoinMethod",
    "CowNumberReused",
    "IdentityConflict",

    # Time
    "EventDay",
    "DaysInMilk",
    "LactationPhase",
    "PreCalvingWeek",
    "CalvingSeason",
    "Parity",
    "LactationStart",
    "LactationEnd",
    "LactationLengthDays",
    "LactationEndSource",

    # Behaviour — 15-min
    "DailyEatingMin15",
    "DailyRuminatingMin15",
    "DailyInactiveMin15",
    "DailyLyingMin15",
    "DailyStandingMin15",
    "DailyWalkingMin15",
    "DailyLegActivity",
    "Eating15minWindows",
    "Lying15minWindows",

    # Behaviour — 2-hr
    "DailyEatingMin2hr",
    "DailyRuminatingMin2hr",
    "DailyInactiveMin2hr",
    "Eating2hrWindows",

    # Behaviour — daily summary
    "DailyEatingMinDay",
    "DailyRuminatingMinDay",
    "DailyInactiveMinDay",
    "LyingBouts",
    "MeanLyingBoutLength",
    "MedianLyingBoutLength",
    "MaxLyingBoutLength",
    "MinLyingBoutLength",
    "NrLyingBouts",
    "LyingBoutLowerQuartile",
    "LyingBoutUpperQuartile",
    "StandupCount",
    "LyingSummary",
    "StandingSummary",
    "WalkingSummary",
    "LegActivitySummary",

    # THI
    "MaxTHIExternal",
    "MeanTHIExternal",
    "HeatStressHoursExternal",
    "SevereHeatStressHoursExternal",
    "CumulativeHeatLoadExternal",
    "MaxTHIInternal",
    "MeanTHIInternal",
    "HeatStressHoursInternal",
    "DailyTHIStatusExternal",
    "DailyTHIStatusInternal",

    # Clinical — calving
    "CalvingType",
    "CalvingTypeLabel",
    "CalvingDurationHr",
    "Afterbirth",
    "AfterbirthLabel",
    "BcsStartDryPeriod",
    "BcsEndDryPeriod",
    "LocoStartDryPeriod",
    "LocoEndDryPeriod",
    "BcsChangeDryPeriod",
    "CalciumLevel",
    "BHBA1",
    "BHBA2",

    # Clinical — composite flags
    "SubclinicalKetosis",
    "SubclinicalHypocalcaemia",
    "DifficultCalving",
    "RetainedPlacenta",

    # Clinical — reproductive
    "CycleWeek4",
    "CycleWeek8",
    "Repro4UterusLabel",
    "Repro8UterusLabel",

    # Reproductive timeline
    "FirstInseminationDate",
    "LastInseminationDate",
    "InseminationCount",
    "DimFirstInsemination",
    "DimLastInsemination",
    "FirstHeatDate",
    "HeatEventCount",
    "DimFirstHeat",
    "FirstPregnancyCheckDate",
    "PregnancyCheckCount",
    "DimFirstPregnancyCheck",
    "KeepOpenCount",
    "DryoffDate",
    "DimDryoff",
    "NextCalvingDate",
    "CalvingIntervalDays",
    "DaysOpen",
    "ConceptionAtFirstInsemination",
    "ReproductiveSuccess",
    "EarlyFirstInsemination",

    # Note flags — all 61
    *sorted([c for c in gold_master.columns if c.startswith("Note")])
)

print(f"Total columns : {len(gold_master.columns)}")
print(f"Total rows    : {gold_master.count():,}")

# COMMAND ----------

# How many distinct BestLifeNumberDigits values in spine match LactationWindows?
spine_lns = spine.select("HerdIdentifier", "BestLifeNumberDigits").distinct()
lw_lns = lw.select(
    F.col("HerdIdentifier").alias("lw_herd"),
    F.col("LifeNumberDigits").alias("lw_digits")
).distinct()

matched = spine_lns.join(
    lw_lns,
    (F.col("HerdIdentifier") == F.col("lw_herd")) &
    (F.col("BestLifeNumberDigits") == F.col("lw_digits")),
    "inner"
).count()

total = spine_lns.count()
print(f"Spine animals with matching LifeNumber in LactationWindows: {matched:,} of {total:,}")

# Also check a specific animal
_spot = lw.select("HerdIdentifier", "AnimalIdentifier").first()
display(lw.where(
    (F.col("HerdIdentifier") == _spot.HerdIdentifier) &
    (F.col("AnimalIdentifier") == _spot.AnimalIdentifier)
).select("HerdIdentifier", "AnimalIdentifier", "LifeNumberDigits", "LactationStart", "LactationEnd"))

# COMMAND ----------

from pyspark.sql import Window as W

# Before dedup: prioritise rows that have a lactation match
# If an animal-day matches both a lactation window and gets null, 
# keep the matched row
w_dedup = W.partitionBy("HerdIdentifier", "AnimalIdentifier", "EventDay").orderBy(
    # Prefer rows with LactationStart (matched) over null (unmatched)
    F.when(F.col("LactationStart").isNotNull(), 0).otherwise(1),
    # Among matched rows, prefer the one with smallest absolute DaysInMilk
    F.abs(F.col("DaysInMilk"))
)

gold_master = (gold_master
    .withColumn("_dedup_rank", F.row_number().over(w_dedup))
    .where(F.col("_dedup_rank") == 1)
    .drop("_dedup_rank"))

print(f"After smart dedup: {gold_master.count():,}")
print(f"Negative DIM rows: {gold_master.where(F.col('DaysInMilk') < 0).count():,}")

# COMMAND ----------

total = gold_master.count()

print("=" * 55)
print("  Gold 7 — GoldMaster Validation")
print("=" * 55)
print(f"\n  Total rows            : {total:>12,}")
print(f"  Columns               : {len(gold_master.columns):>12,}")
print()

# Coverage
print("  Data coverage:")
display(gold_master.select(
    F.round(F.sum(F.col("HasRefData").cast("int")) / total * 100, 1)
      .alias("PctWithRefData"),
    F.round(F.sum(F.col("LactationStart").isNotNull().cast("int")) / total * 100, 1)
      .alias("PctWithLactationWindow"),
    F.round(F.sum(F.col("MaxTHIExternal").isNotNull().cast("int")) / total * 100, 1)
      .alias("PctWithTHI"),
    F.round(F.sum(F.col("SubclinicalKetosis").isNotNull().cast("int")) / total * 100, 1)
      .alias("PctWithClinicalData"),
    F.round(F.sum(F.col("InseminationCount").isNotNull().cast("int")) / total * 100, 1)
      .alias("PctWithReproData"),
))

# Rows per farm
print("\n  Rows per farm:")
display(gold_master
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("TotalDays"),
        F.countDistinct("AnimalIdentifier").alias("Animals"),
        F.sum(F.col("HasRefData").cast("int")).alias("DaysWithRefData"),
        F.sum(F.col("LactationStart").isNotNull().cast("int"))
          .alias("DaysInLactationWindow")
    )
    .orderBy("HerdIdentifier"))

# LactationPhase distribution
print("\n  LactationPhase distribution:")
display(gold_master
    .groupBy("LactationPhase")
    .count()
    .orderBy("LactationPhase"))

# DIM distribution
print("\n  DaysInMilk distribution (matched rows only):")
display(gold_master
    .where(F.col("DaysInMilk").isNotNull())
    .select(
        F.min("DaysInMilk").alias("Min"),
        F.percentile_approx("DaysInMilk", 0.25).alias("Q1"),
        F.percentile_approx("DaysInMilk", 0.5).alias("Median"),
        F.percentile_approx("DaysInMilk", 0.75).alias("Q3"),
        F.max("DaysInMilk").alias("Max")
    ))

print("\n  Sample rows:")
display(gold_master
    .where(F.col("HasRefData") == True)
    .orderBy("HerdIdentifier", "AnimalIdentifier", "EventDay")
    .limit(5))

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"

(gold_master
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("HerdIdentifier")
    .save(f"{GOLD_VOLUME}/GoldMaster"))

rows = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster").count()
print(f"WROTE {rows:,} rows -> {GOLD_VOLUME}/GoldMaster")

# COMMAND ----------

gm = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")

print("=" * 55)
print("  Gold 7 — GoldMaster Summary")
print("=" * 55)
print(f"  Total rows            : {gm.count():>12,}")
print(f"  Distinct animals      : {gm.select('AnimalIdentifier').distinct().count():>12,}")
print(f"  Distinct farms        : {gm.select('HerdIdentifier').distinct().count():>12,}")
print(f"  Columns               : {len(gm.columns):>12,}")
print(f"  Date range            : {gm.select(F.min('EventDay').alias('Min'), F.max('EventDay').alias('Max')).first()}")

# COMMAND ----------

gm = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")

print("DIM distribution (negative = pre-calving):")
display(gm
    .where(F.col("DaysInMilk").isNotNull())
    .select(
        F.min("DaysInMilk").alias("MinDIM"),
        F.max("DaysInMilk").alias("MaxDIM"),
        F.sum((F.col("DaysInMilk") < 0).cast("int")).alias("PreCalvingRows"),
        F.sum((F.col("DaysInMilk") >= 0).cast("int")).alias("PostCalvingRows"),
    ))

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"

tables = [
    "LactationWindows",
    "AnimalIdentityBridge", 
    "DailyBehaviourSummary",
    "DailyAnimalIdentity",
    "DailyTHISummary",
    "ClinicalFlags",
    "ReproductiveTimeline",
    "GoldMaster",
]

from pyspark.sql import functions as F

for table in tables:
    path = f"{GOLD_VOLUME}/{table}"
    try:
        df = spark.read.format("delta").load(path)
        farms = sorted([r[0] for r in df.select("HerdIdentifier").distinct().collect()])
        ok = all(f.startswith("Farm_") for f in farms)
        print(f"{'✅' if ok else '❌'} {table}: {farms}")
    except Exception as e:
        print(f" {table}: NOT IN VOLUME — {e}")

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"
gm = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")

# How many cow-days had a reused barn number?
reused_days = gm.filter(F.col("CowNumberReused") == True).count()
print(f"Cow-days with reused barn number: {reused_days}")

# How many distinct barn numbers were reused, per farm?
reused_animals = (
    gm.filter(F.col("CowNumberReused") == True)
    .select("HerdIdentifier", "AnimalIdentifier")
    .distinct()
    .groupBy("HerdIdentifier")
    .count()
    .orderBy("HerdIdentifier")
)
reused_animals.show()

# Spot-check: show the date split for one reused cow
(
    gm.filter(F.col("CowNumberReused") == True)
    .select("HerdIdentifier", "AnimalIdentifier", "EventDay",
            "BestLifeNumberDigits", "JoinMethod", "LactationStart")
    .orderBy("HerdIdentifier", "AnimalIdentifier", "EventDay")
    .limit(30)
    .show(truncate=False)
)

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"
gm = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")

# Print all column names so we can find the right one
print(gm.columns)

# COMMAND ----------

# How many cow-days by identity method
gm.groupBy("JoinMethod").count().orderBy("count", ascending=False).show()

# HasRefData = False means no national ID match at all
gm.groupBy("HasRefData").count().show()

# Unmatched cows per farm
(
    gm.filter(F.col("HasRefData") == False)
    .select("HerdIdentifier", "AnimalIdentifier")
    .distinct()
    .groupBy("HerdIdentifier")
    .count()
    .orderBy("HerdIdentifier")
    .show()
)

# COMMAND ----------

dai = spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyAnimalIdentity")
print(dai.columns)  # confirm the column name first

dai.groupBy("CowNumberReused").count().show()

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"
gm = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")

# 1. Confirm the two new columns are there
print("CowNumberReused" in gm.columns)   # should print True
print("IdentityConflict" in gm.columns)  # should print True

# 2. How many cow-days had a reused barn number?
print("\nCowNumberReused distribution:")
gm.groupBy("CowNumberReused").count().orderBy("CowNumberReused").show()

# 3. IdentityConflict distribution
print("IdentityConflict distribution:")
gm.groupBy("IdentityConflict").count().orderBy("IdentityConflict").show()

# 4. Reused barn numbers per farm
print("Reused barn numbers per farm:")
(
    gm.filter(F.col("CowNumberReused") == True)
    .select("HerdIdentifier", "AnimalIdentifier")
    .distinct()
    .groupBy("HerdIdentifier")
    .count()
    .orderBy("HerdIdentifier")
    .show()
)

# 5. Row count unchanged?
print(f"Total rows: {gm.count():,}")  # should still be 2,530,267

# COMMAND ----------

# Are conflict days near a CowNumberReused transition?
(
    gm.filter(F.col("IdentityConflict") == True)
    .groupBy("CowNumberReused", "HerdIdentifier")
    .count()
    .orderBy("HerdIdentifier")
    .show()
)