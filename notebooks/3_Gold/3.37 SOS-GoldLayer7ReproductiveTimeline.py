# Databricks notebook source
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG       = dbutils.widgets.get("catalog")
SILVER_VOLUME = dbutils.widgets.get("silver_volume").rstrip("/")
GOLD_SCHEMA   = f"{CATALOG}.gold"
SILVER_SCHEMA = f"{CATALOG}.silver"

OUT_TABLE = f"{GOLD_SCHEMA}.ReproductiveTimeline"

print("Writing to :", OUT_TABLE)

# COMMAND ----------

# Load relevant reproductive event types from CalendarData silver
REPRO_EVENTS = [
    "InseminationEvent",
    "HeatEvent",
    "PregnancyCheckEvent",
    "KeepOpenEvent",
    "DryoffEvent",
    "CalvingEvent"
]

calendar = (spark.table(f"{SILVER_SCHEMA}.calendardata")
    .where(F.col("ObservationType").isin(REPRO_EVENTS))
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        "ObservationType",
        F.to_date("EventDate").alias("EventDate")
    ))

lw = spark.table(f"{GOLD_SCHEMA}.LactationWindows").select(
    "HerdIdentifier",
    "AnimalIdentifier",
    "LifeNumber",
    "LifeNumberDigits",
    "Parity",
    "LactationStart",
    "LactationEnd",
    "LactationEndSource",
    "LactationLengthDays"
)

print(f"Calendar reproductive events : {calendar.count():,}")
print(f"Lactation windows            : {lw.count():,}")
print()
print("Event counts by type:")
display(calendar.groupBy("ObservationType").count().orderBy("ObservationType"))

# COMMAND ----------

# Split into two joins:
# 1. All repro events within lactation window
# 2. CalvingEvents with extended window (up to 60 days after LactationEnd)

REPRO_NON_CALVING = [
    "InseminationEvent", "HeatEvent", "PregnancyCheckEvent",
    "KeepOpenEvent", "DryoffEvent"
]

# Non-calving events — must fall within lactation window
repro_in_window = (calendar
    .where(F.col("ObservationType").isin(REPRO_NON_CALVING))
    .alias("c")
    .join(
        lw.alias("lw"),
        (F.col("c.HerdIdentifier")   == F.col("lw.HerdIdentifier")) &
        (F.col("c.AnimalIdentifier") == F.col("lw.AnimalIdentifier")) &
        (F.col("c.EventDate")        >= F.col("lw.LactationStart")) &
        (F.col("c.EventDate")        <= F.col("lw.LactationEnd")),
        "inner"
    )
    .select(
        F.col("lw.HerdIdentifier"),
        F.col("lw.AnimalIdentifier"),
        F.col("lw.LifeNumber"),
        F.col("lw.LifeNumberDigits"),
        F.col("lw.Parity"),
        F.col("lw.LactationStart"),
        F.col("lw.LactationEnd"),
        F.col("lw.LactationLengthDays"),
        F.col("c.ObservationType"),
        F.col("c.EventDate"),
        F.datediff(F.col("c.EventDate"), F.col("lw.LactationStart"))
         .alias("DaysInMilkAtEvent")
    ))

# CalvingEvents — extended window up to 60 days after LactationEnd
# to capture the next calving which signals end of reproductive cycle
calving_events = (calendar
    .where(F.col("ObservationType") == "CalvingEvent")
    .alias("c")
    .join(
        lw.alias("lw"),
        (F.col("c.HerdIdentifier")   == F.col("lw.HerdIdentifier")) &
        (F.col("c.AnimalIdentifier") == F.col("lw.AnimalIdentifier")) &
        # Next calving must be after LactationStart + 100 days minimum
        # and within 60 days after LactationEnd
        (F.col("c.EventDate") > F.date_add(F.col("lw.LactationStart"), 100)) &
        (F.col("c.EventDate") <= F.date_add(F.col("lw.LactationEnd"), 60)),
        "inner"
    )
    .select(
        F.col("lw.HerdIdentifier"),
        F.col("lw.AnimalIdentifier"),
        F.col("lw.LifeNumber"),
        F.col("lw.LifeNumberDigits"),
        F.col("lw.Parity"),
        F.col("lw.LactationStart"),
        F.col("lw.LactationEnd"),
        F.col("lw.LactationLengthDays"),
        F.col("c.ObservationType"),
        F.col("c.EventDate"),
        F.datediff(F.col("c.EventDate"), F.col("lw.LactationStart"))
         .alias("DaysInMilkAtEvent")
    ))

# Union both
repro_in_window = repro_in_window.unionByName(calving_events)

print(f"Reproductive events matched to lactations : {repro_in_window.count():,}")
print()
print("Matched events by type:")
display(repro_in_window
    .groupBy("ObservationType")
    .agg(
        F.count("*").alias("Events"),
        F.min("DaysInMilkAtEvent").alias("MinDIM"),
        F.avg("DaysInMilkAtEvent").alias("AvgDIM"),
        F.max("DaysInMilkAtEvent").alias("MaxDIM")
    )
    .orderBy("ObservationType"))

# COMMAND ----------

# Aggregate events to one row per cow-lactation
# For each event type: first occurrence, last occurrence, count

w_asc  = Window.partitionBy(
    "HerdIdentifier", "AnimalIdentifier", "LactationStart", "ObservationType"
).orderBy("EventDate")

w_desc = Window.partitionBy(
    "HerdIdentifier", "AnimalIdentifier", "LactationStart", "ObservationType"
).orderBy(F.desc("EventDate"))

repro_timeline = (repro_in_window
    .groupBy(
        "HerdIdentifier", "AnimalIdentifier", "LifeNumber",
        "LifeNumberDigits", "Parity", "LactationStart", "LactationEnd",
        "LactationLengthDays"
    )
    .agg(
        # Insemination
        F.min(F.when(F.col("ObservationType") == "InseminationEvent",
                     F.col("EventDate"))).alias("FirstInseminationDate"),
        F.max(F.when(F.col("ObservationType") == "InseminationEvent",
                     F.col("EventDate"))).alias("LastInseminationDate"),
        F.sum(F.when(F.col("ObservationType") == "InseminationEvent",
                     F.lit(1)).otherwise(0)).alias("InseminationCount"),
        F.min(F.when(F.col("ObservationType") == "InseminationEvent",
                     F.col("DaysInMilkAtEvent"))).alias("DimFirstInsemination"),
        F.max(F.when(F.col("ObservationType") == "InseminationEvent",
                     F.col("DaysInMilkAtEvent"))).alias("DimLastInsemination"),

        # Heat detection
        F.min(F.when(F.col("ObservationType") == "HeatEvent",
                     F.col("EventDate"))).alias("FirstHeatDate"),
        F.sum(F.when(F.col("ObservationType") == "HeatEvent",
                     F.lit(1)).otherwise(0)).alias("HeatEventCount"),
        F.min(F.when(F.col("ObservationType") == "HeatEvent",
                     F.col("DaysInMilkAtEvent"))).alias("DimFirstHeat"),

        # Pregnancy check
        F.min(F.when(F.col("ObservationType") == "PregnancyCheckEvent",
                     F.col("EventDate"))).alias("FirstPregnancyCheckDate"),
        F.max(F.when(F.col("ObservationType") == "PregnancyCheckEvent",
                     F.col("EventDate"))).alias("LastPregnancyCheckDate"),
        F.sum(F.when(F.col("ObservationType") == "PregnancyCheckEvent",
                     F.lit(1)).otherwise(0)).alias("PregnancyCheckCount"),
        F.min(F.when(F.col("ObservationType") == "PregnancyCheckEvent",
                     F.col("DaysInMilkAtEvent"))).alias("DimFirstPregnancyCheck"),

        # Keep open
        F.sum(F.when(F.col("ObservationType") == "KeepOpenEvent",
                     F.lit(1)).otherwise(0)).alias("KeepOpenCount"),

        # Dryoff
        F.min(F.when(F.col("ObservationType") == "DryoffEvent",
                     F.col("EventDate"))).alias("DryoffDate"),
        F.min(F.when(F.col("ObservationType") == "DryoffEvent",
                     F.col("DaysInMilkAtEvent"))).alias("DimDryoff"),

        # Next calving — exclude DIM 0 (current calving), take minimum after DIM 0
        F.min(F.when(
            (F.col("ObservationType") == "CalvingEvent") &
            (F.col("DaysInMilkAtEvent") > 0),
            F.col("EventDate"))).alias("NextCalvingDate"),
        F.min(F.when(
            (F.col("ObservationType") == "CalvingEvent") &
            (F.col("DaysInMilkAtEvent") > 0),
            F.col("DaysInMilkAtEvent"))).alias("DimNextCalving"),
    ))

print(f"Lactations with reproductive data : {repro_timeline.count():,}")

# COMMAND ----------

# Calving interval = days between LactationStart and NextCalvingDate
# Days open = days from LactationStart to first confirmed pregnancy
# Conception rate proxy = was an insemination followed by a calving?

repro_timeline = (repro_timeline
    # Calving interval in days
    .withColumn("CalvingIntervalDays",
        F.when(F.col("NextCalvingDate").isNotNull(),
               F.datediff(F.col("NextCalvingDate"), F.col("LactationStart")))
    )

    # Days open — days from calving to last insemination
    # (proxy for days to confirmed pregnancy)
    .withColumn("DaysOpen",
        F.when(F.col("LastInseminationDate").isNotNull(),
               F.datediff(F.col("LastInseminationDate"), F.col("LactationStart")))
    )

    # Conception efficiency — fewer inseminations is better
    .withColumn("ConceptionAtFirstInsemination",
        F.col("InseminationCount") == 1
    )

    # Reproductive success — did this lactation result in a next calving?
    .withColumn("ReproductiveSuccess",
        F.col("NextCalvingDate").isNotNull()
    )

    # Voluntary waiting period end — typically 50 DIM for dairy cows
    # First insemination before DIM 50 may indicate early breeding
    .withColumn("EarlyFirstInsemination",
        F.col("DimFirstInsemination") < 50
    )
)

print("Derived research variables added")
print(f"Total lactations : {repro_timeline.count():,}")

# COMMAND ----------

print("Reproductive timeline coverage per farm:")
display(repro_timeline
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("Lactations"),
        F.sum((F.col("InseminationCount") > 0).cast("int"))
          .alias("WithInsemination"),
        F.sum(F.col("ReproductiveSuccess").cast("int"))
          .alias("ReproductiveSuccess"),
        F.round(F.avg("InseminationCount"), 2)
          .alias("AvgInseminations"),
        F.round(F.avg("DaysOpen"), 1)
          .alias("AvgDaysOpen"),
        F.round(F.avg("CalvingIntervalDays"), 1)
          .alias("AvgCalvingInterval")
    )
    .orderBy("HerdIdentifier"))

print("\nInsemination count distribution:")
display(repro_timeline
    .groupBy("InseminationCount")
    .count()
    .orderBy("InseminationCount"))

print("\nConception at first insemination:")
display(repro_timeline
    .groupBy("ConceptionAtFirstInsemination")
    .count()
    .orderBy("ConceptionAtFirstInsemination"))

print("\nDIM at first insemination distribution:")
display(repro_timeline.select(
    F.min("DimFirstInsemination").alias("Min"),
    F.percentile_approx("DimFirstInsemination", 0.25).alias("Q1"),
    F.percentile_approx("DimFirstInsemination", 0.5).alias("Median"),
    F.percentile_approx("DimFirstInsemination", 0.75).alias("Q3"),
    F.max("DimFirstInsemination").alias("Max"),
    F.avg("DimFirstInsemination").alias("Avg")
))

print("\nSample rows:")
display(repro_timeline
    .orderBy("HerdIdentifier", "AnimalIdentifier", "LactationStart")
    .limit(10))

# COMMAND ----------

(repro_timeline
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")

# COMMAND ----------

gold_rt = spark.table(OUT_TABLE)

print("=" * 55)
print("  Gold 6 — ReproductiveTimeline Summary")
print("=" * 55)
print(f"  Total lactations          : {gold_rt.count():>10,}")
print(f"  With insemination data    : {gold_rt.where(F.col('InseminationCount') > 0).count():>10,}")
print(f"  With pregnancy check data : {gold_rt.where(F.col('PregnancyCheckCount') > 0).count():>10,}")
print(f"  Reproductive success      : {gold_rt.where(F.col('ReproductiveSuccess')).count():>10,}")
print(f"  Distinct farms            : {gold_rt.select('HerdIdentifier').distinct().count():>10,}")
print(f"  Columns                   : {len(gold_rt.columns):>10,}")

# COMMAND ----------

spark.table("senseofsensors_catalog.gold.ReproductiveTimeline") \
    .select("HerdIdentifier").distinct().orderBy("HerdIdentifier").show()