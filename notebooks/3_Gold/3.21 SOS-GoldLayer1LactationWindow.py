# Databricks notebook source
#Load Silver Layer

from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")
dbutils.widgets.text("gold_volume",   "/Volumes/senseofsensors_catalog/gold/gold_volume")

CATALOG        = dbutils.widgets.get("catalog")
SILVER_VOLUME  = dbutils.widgets.get("silver_volume").rstrip("/")
GOLD_VOLUME    = dbutils.widgets.get("gold_volume").rstrip("/")
GOLD_SCHEMA    = f"{CATALOG}.gold"
SILVER_SCHEMA  = f"{CATALOG}.silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

SRC_PATH  = f"{SILVER_VOLUME}/ReferenceData"
OUT_TABLE = f"{GOLD_SCHEMA}.LactationWindows"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

print("Gold schema  :", GOLD_SCHEMA)
print("Reading from :", SRC_PATH)
print("Writing to   :", OUT_TABLE)

# COMMAND ----------

ref = spark.read.format("delta").load(SRC_PATH)

print(f"ReferenceData rows : {ref.count():,}")
print(f"Columns            : {len(ref.columns)}")
print()

# Confirm the columns we need are present
required = ["HerdIdentifier", "AnimalIdentifier", "LifeNumber",
            "LifeNumberDigits", "Parity", "CalvingDate"]
missing  = [c for c in required if c not in ref.columns]
if missing:
    raise ValueError(f"Missing required columns: {missing}")
print("All required columns present")

# COMMAND ----------

# some LifeNumber values missing space
# after country prefix e.g. "NL123456789" instead of "NL 123456789"
# Fix: insert space between 2-letter prefix and digits if missing

ref = ref.withColumn(
    "LifeNumber",
    F.regexp_replace(
        F.col("LifeNumber"),
        r"^([A-Z]{2})(\d)",
        "$1 $2"
    )
).withColumn(
    "LifeNumberDigits",
    F.regexp_replace(F.col("LifeNumber"), r"\D", "")
)

# Verify fix
print("Sample after fix:")
display(ref
    .select("HerdIdentifier", "AnimalIdentifier", "LifeNumber", "LifeNumberDigits")
    .where(F.col("LifeNumber").isNotNull())
    .limit(10))

# COMMAND ----------

# Lactation windows require a CalvingDate anchor.
# Rows without CalvingDate cannot form a window — excluded here.
# They remain in ReferenceData silver and will join to GoldMaster via identity.

before = ref.count()
ref_with_calving = ref.where(F.col("CalvingDate").isNotNull())
after  = ref_with_calving.count()

print(f"Rows with CalvingDate    : {after:,}")
print(f"Rows without CalvingDate : {before - after:,}  (excluded from windows; kept in silver)")

# COMMAND ----------

w = Window.partitionBy("HerdIdentifier", "AnimalIdentifier").orderBy("CalvingDate")

lactation_windows = (ref_with_calving
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        "LifeNumber",
        "LifeNumberDigits",
        "Parity",
        "CalvingDate",
        "CalvingType",
        "CalvingTypeLabel",
        "CalvingSignal",
        "CalvingDurationHr",
        "CalfRemoval",
        "CalfRemovalLabel",
        "Afterbirth",
        "AfterbirthLabel",
        "BcsStartDryPeriod",
        "BcsEndDryPeriod",
        "LocoStartDryPeriod",
        "LocoEndDryPeriod",
        "CalciumLevel",
        "DimCalcium",
        "BHBA1",
        "DimBhba1",
        "BHBA2",
        "DimBhba2",
        "CycleWeek4",
        "CycleWeek8",
        "Repro4Uterus",
        "Repro4UterusLabel",
        "Repro8Uterus",
        "Repro8UterusLabel",
        "Notes",
        "IndividualAttention1",
        "IndividualAttention2",
        "IndividualAttention3",
        "IndividualAttention4",
        "ClinicalObservationsOverflow",
    )
    .withColumn("LactationStart", F.col("CalvingDate"))
    .withColumn("LactationEndEstimated",
        F.coalesce(
            F.lead("CalvingDate", 1).over(w),
            F.date_add(F.col("CalvingDate"), 365)
        )
    )
    .drop("CalvingDate")
)

# Load DryoffEvents
dryoff = (spark.table(f"{SILVER_SCHEMA}.calendardata")
    .where(F.col("ObservationType") == "DryoffEvent")
    .select(
        F.col("HerdIdentifier").alias("d_HerdIdentifier"),
        F.col("AnimalIdentifier").alias("d_AnimalIdentifier"),
        F.col("EventDate").alias("DryoffDate")
    ))

# Join on all three keys — left join keeps ALL lactation windows
lactation_windows = (lactation_windows
    .join(
        dryoff,
        (F.col("HerdIdentifier")       == F.col("d_HerdIdentifier")) &
        (F.col("AnimalIdentifier")     == F.col("d_AnimalIdentifier")) &
        (F.col("DryoffDate")           >= F.col("LactationStart")) &
        (F.col("DryoffDate")           <= F.col("LactationEndEstimated")),
        "left"
    )
    .drop("d_HerdIdentifier", "d_AnimalIdentifier")
)

# Pick latest DryoffDate within each lactation window
w2 = Window.partitionBy("HerdIdentifier", "AnimalIdentifier", "LactationStart")

lactation_windows = (lactation_windows
    .withColumn("DryoffDate",
        F.last(F.col("DryoffDate"), ignorenulls=True).over(
            w2.orderBy("DryoffDate")
        )
    )
    .dropDuplicates(["HerdIdentifier", "AnimalIdentifier", "LactationStart"])
    .withColumn("LactationEnd",
        F.when(F.col("DryoffDate").isNotNull(), F.col("DryoffDate"))
         .otherwise(F.col("LactationEndEstimated"))
    )
    .withColumn("LactationEndSource",
        F.when(F.col("DryoffDate").isNotNull(), F.lit("DryoffEvent"))
         .otherwise(F.lit("Estimated"))
    )
    .withColumn("LactationLengthDays",
        F.datediff(F.col("LactationEnd"), F.col("LactationStart"))
    )
    .drop("DryoffDate", "LactationEndEstimated")
)

print(f"Total windows          : {lactation_windows.count():,}")
print(f"With actual DryoffDate : {lactation_windows.where(F.col('LactationEndSource') == 'DryoffEvent').count():,}")
print(f"Still estimated        : {lactation_windows.where(F.col('LactationEndSource') == 'Estimated').count():,}")

# COMMAND ----------

# Add clinical risk flags directly on the lactation window table.
# These are the composite variables used in the research papers.

lactation_windows = (lactation_windows
    # Subclinical ketosis: BHBA >= 1.2 mmol/L at either sample
    .withColumn("SubclinicalKetosis",
        (F.col("BHBA1") >= 1.2) | (F.col("BHBA2") >= 1.2))

    # Subclinical hypocalcaemia: calcium < 2.0 mmol/L
    .withColumn("SubclinicalHypocalcaemia",
        F.col("CalciumLevel") < 2.0)

    # Difficult calving: type 3 or 4
    .withColumn("DifficultCalving",
        F.col("CalvingType").isin(["3", "4"]))

    # Retained placenta: afterbirth code 2, 3, or 4
    .withColumn("RetainedPlacenta",
        F.col("Afterbirth").isin(["2", "3", "4"]))

    # Body condition loss: BCS at dry-off minus BCS at end of dry period
    # Negative = loss, positive = gain
    .withColumn("BcsChangeDryPeriod",
        F.when(
            F.col("BcsStartDryPeriod").isNotNull() &
            F.col("BcsEndDryPeriod").isNotNull(),
            F.col("BcsEndDryPeriod") - F.col("BcsStartDryPeriod")
        )
    )
)

print("Composite columns added:")
print("  SubclinicalKetosis, SubclinicalHypocalcaemia, DifficultCalving,")
print("  RetainedPlacenta, BcsChangeDryPeriod")

# COMMAND ----------

print("Schema:")
lactation_windows.printSchema()

print(f"\nTotal windows        : {lactation_windows.count():,}")
print(f"Animals with >1 lact : {lactation_windows.groupBy('HerdIdentifier','AnimalIdentifier').count().where(F.col('count')>1).count():,}")

print("\nWindows per farm:")
display(lactation_windows
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("LactationWindows"),
        F.countDistinct("AnimalIdentifier").alias("Animals"),
        F.min("LactationStart").alias("EarliestCalving"),
        F.max("LactationStart").alias("LatestCalving")
    )
    .orderBy("HerdIdentifier"))

print("\nComposite flag counts:")
display(lactation_windows.select(
    F.sum(F.col("SubclinicalKetosis").cast("int")).alias("SubclinicalKetosis"),
    F.sum(F.col("SubclinicalHypocalcaemia").cast("int")).alias("SubclinicalHypocalcaemia"),
    F.sum(F.col("DifficultCalving").cast("int")).alias("DifficultCalving"),
    F.sum(F.col("RetainedPlacenta").cast("int")).alias("RetainedPlacenta"),
    F.count(F.when(F.col("BcsChangeDryPeriod").isNotNull(), 1)).alias("BcsChangeAvailable"),
))

print("\nLactation length distribution:")
display(lactation_windows.select(
    F.min("LactationLengthDays").alias("MinDays"),
    F.percentile_approx("LactationLengthDays", 0.25).alias("Q1Days"),
    F.percentile_approx("LactationLengthDays", 0.5).alias("MedianDays"),
    F.percentile_approx("LactationLengthDays", 0.75).alias("Q3Days"),
    F.max("LactationLengthDays").alias("MaxDays"),
))

print("\nSample rows:")
display(lactation_windows.orderBy("HerdIdentifier", "AnimalIdentifier", "LactationStart").limit(20))

# COMMAND ----------

lactation_windows = lactation_windows.withColumn(
    "LactationEnd", F.to_date(F.col("LactationEnd"))
)

# COMMAND ----------

(lactation_windows
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")

# COMMAND ----------

gold_lw = spark.table(OUT_TABLE)

print("=" * 55)
print("  Gold 1 — LactationWindows Summary")
print("=" * 55)
print(f"  Total lactation windows : {gold_lw.count():>10,}")
print(f"  Distinct animals        : {gold_lw.select('AnimalIdentifier').distinct().count():>10,}")
print(f"  Distinct farms          : {gold_lw.select('HerdIdentifier').distinct().count():>10,}")
print(f"  Columns                 : {len(gold_lw.columns):>10,}")
print()
print("  Parity distribution:")
display(gold_lw.groupBy("Parity").count().orderBy("Parity"))

# COMMAND ----------

# Check if any animal-lactation combinations are duplicated
display(spark.table("senseofsensors_catalog.gold.LactationWindows")
    .groupBy("HerdIdentifier", "AnimalIdentifier", "LactationStart")
    .count()
    .where(F.col("count") > 1)
    .count())

# COMMAND ----------

display(spark.table("senseofsensors_catalog.gold.LactationWindows")
    .select("HerdIdentifier").distinct().orderBy("HerdIdentifier"))

# COMMAND ----------

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"
lw = spark.table("senseofsensors_catalog.gold.LactationWindows")
(lw.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_VOLUME}/LactationWindows"))
print("Written to Volume path")

# COMMAND ----------

display(spark.read.format("delta").load(f"{GOLD_VOLUME}/LactationWindows")
    .select("HerdIdentifier").distinct().orderBy("HerdIdentifier"))