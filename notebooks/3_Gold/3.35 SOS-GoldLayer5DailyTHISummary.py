# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG       = dbutils.widgets.get("catalog")
SILVER_VOLUME = dbutils.widgets.get("silver_volume").rstrip("/")
GOLD_SCHEMA   = f"{CATALOG}.gold"

THI_PATH  = f"{SILVER_VOLUME}/TemperatureHumidityIndex"
OUT_TABLE = f"{GOLD_SCHEMA}.DailyTHISummary"

print("Reading from :", THI_PATH)
print("Writing to   :", OUT_TABLE)

# COMMAND ----------

thi = spark.read.format("delta").load(THI_PATH)

print(f"THI rows    : {thi.count():,}")
print(f"Columns     : {thi.columns}")
print()
display(thi.limit(5))

# COMMAND ----------

# Aggregate hourly THI to daily per farm
# Internal columns are null for farms with no indoor sensor — handled with coalesce

daily_thi = (thi
    .groupBy("HerdIdentifier", "MeasurementDate")
    .agg(
        # External THI
        F.max("THIExternal").alias("MaxTHIExternal"),
        F.min("THIExternal").alias("MinTHIExternal"),
        F.round(F.avg("THIExternal"), 2).alias("MeanTHIExternal"),

        # Heat stress hours external — hours above THI 72
        F.sum(
            F.when(F.col("THIExternal") > 72, 1).otherwise(0)
        ).alias("HeatStressHoursExternal"),

        # Severe heat stress hours external — hours above THI 80
        F.sum(
            F.when(F.col("THIExternal") > 80, 1).otherwise(0)
        ).alias("SevereHeatStressHoursExternal"),

        # Cumulative heat load external — sum of (THI - 72) when above 72
        F.round(F.sum(
            F.when(F.col("THIExternal") > 72,
                   F.col("THIExternal") - 72)
             .otherwise(0)
        ), 2).alias("CumulativeHeatLoadExternal"),

        # Internal THI — null for farms with no indoor sensor
        F.max("THIInternal").alias("MaxTHIInternal"),
        F.min("THIInternal").alias("MinTHIInternal"),
        F.round(F.avg("THIInternal"), 2).alias("MeanTHIInternal"),

        F.sum(
            F.when(F.col("THIInternal") > 72, 1).otherwise(0)
        ).alias("HeatStressHoursInternal"),

        F.sum(
            F.when(F.col("THIInternal") > 80, 1).otherwise(0)
        ).alias("SevereHeatStressHoursInternal"),

        F.round(F.sum(
            F.when(F.col("THIInternal") > 72,
                   F.col("THIInternal") - 72)
             .otherwise(0)
        ), 2).alias("CumulativeHeatLoadInternal"),

        # Count of hourly readings used in aggregation
        F.count("*").alias("HourlyReadingsCount")
    ))

print(f"Daily THI rows : {daily_thi.count():,}")

# COMMAND ----------

# DailyTHIStatusExternal based on MaxTHI for the day
# Using standard dairy literature thresholds:
# < 50 = NoStress, 50-72 = Mild, 72-80 = HeatStress, > 80 = SevereHeatStress

daily_thi = daily_thi.withColumn("DailyTHIStatusExternal",
    F.when(F.col("MaxTHIExternal").isNull(),  None)
     .when(F.col("MaxTHIExternal") > 80,      F.lit("SevereHeatStress"))
     .when(F.col("MaxTHIExternal") > 72,      F.lit("HeatStress"))
     .when(F.col("MaxTHIExternal") >= 50,     F.lit("Mild"))
     .otherwise(F.lit("NoStress"))
).withColumn("DailyTHIStatusInternal",
    F.when(F.col("MaxTHIInternal").isNull(),  None)
     .when(F.col("MaxTHIInternal") > 80,      F.lit("SevereHeatStress"))
     .when(F.col("MaxTHIInternal") > 72,      F.lit("HeatStress"))
     .when(F.col("MaxTHIInternal") >= 50,     F.lit("Mild"))
     .otherwise(F.lit("NoStress"))
)

print("Heat stress status added")
display(daily_thi.limit(5))

# COMMAND ----------

print("Date range per farm:")
display(daily_thi
    .groupBy("HerdIdentifier")
    .agg(
        F.min("MeasurementDate").alias("EarliestDate"),
        F.max("MeasurementDate").alias("LatestDate"),
        F.count("*").alias("Days"),
        F.sum((F.col("HeatStressHoursExternal") > 0).cast("int"))
          .alias("DaysWithHeatStress"),
        F.max("MaxTHIExternal").alias("MaxTHIEver")
    )
    .orderBy("HerdIdentifier"))

print("\nDaily THI status distribution:")
display(daily_thi
    .groupBy("DailyTHIStatusExternal")
    .count()
    .orderBy("DailyTHIStatusExternal"))

print("\nNull rates — internal THI (expected for external-only farms):")
total = daily_thi.count()
display(daily_thi.select(
    F.round(F.sum(F.col("MaxTHIInternal").isNull().cast("int")) / total * 100, 1)
     .alias("MaxTHIInternal_NullPct"),
    F.round(F.sum(F.col("MeanTHIInternal").isNull().cast("int")) / total * 100, 1)
     .alias("MeanTHIInternal_NullPct"),
))

print("\nSample rows:")
display(daily_thi
    .orderBy("HerdIdentifier", "MeasurementDate")
    .limit(10))

# COMMAND ----------

(daily_thi
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("HerdIdentifier")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")

# COMMAND ----------

gold_thi = spark.table(OUT_TABLE)

print("=" * 55)
print("  Gold 4 — DailyTHISummary Validation")
print("=" * 55)
print(f"  Total farm-days   : {gold_thi.count():>10,}")
print(f"  Distinct farms    : {gold_thi.select('HerdIdentifier').distinct().count():>10,}")
print(f"  Distinct days     : {gold_thi.select('MeasurementDate').distinct().count():>10,}")
print(f"  Columns           : {len(gold_thi.columns):>10,}")
print()
print("  Duplicate farm-days:")
dupes = gold_thi.groupBy("HerdIdentifier", "MeasurementDate").count().where(F.col("count") > 1).count()
print(f"  {dupes:,}  {'ok' if dupes == 0 else 'PROBLEM'}")

# COMMAND ----------

gold_thi = spark.table("senseofsensors_catalog.gold.DailyTHISummary")

print("Distinct HerdIdentifiers:")
gold_thi.select("HerdIdentifier").distinct().orderBy("HerdIdentifier").show()

print(f"Total rows: {gold_thi.count():,}")
dupes = gold_thi.groupBy("HerdIdentifier", "MeasurementDate").count().where("count > 1").count()
print(f"Dupes: {dupes}  {'ok' if dupes == 0 else 'PROBLEM'}")