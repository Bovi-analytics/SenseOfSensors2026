# Databricks notebook source
spark.sql("""
    CREATE VOLUME IF NOT EXISTS senseofsensors_catalog.gold.gold_volume
""")

print(" Gold volume created")

# Verify
dbutils.fs.ls("/Volumes/senseofsensors_catalog/gold/gold_volume")
print(" Volume accessible")

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG      = "senseofsensors_catalog"
GOLD_SCHEMA  = f"{CATALOG}.gold"
GOLD_VOLUME  = "/Volumes/senseofsensors_catalog/gold/gold_volume"

TABLES = [
    "LactationWindows",
    "AnimalIdentityBridge",
    "DailyBehaviourSummary",
    "DailyTHISummary",
    "ClinicalFlags",
    "ReproductiveTimeline",
    "GoldMaster",
]

print("Gold volume :", GOLD_VOLUME)
print("Tables to migrate:", TABLES)

# Verify volume exists
try:
    dbutils.fs.ls(GOLD_VOLUME)
    print(" Gold volume exists")
except:
    raise Exception(" Gold volume does not exist — create it in Catalog UI first")

# COMMAND ----------

for table in TABLES:
    src_table = f"{GOLD_SCHEMA}.{table}"
    out_path  = f"{GOLD_VOLUME}/{table}"

    print(f"\nMigrating {table}...")

    # Read from managed table
    df = spark.table(src_table)
    rows = df.count()

    # Write to volume
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out_path))

    print(f"  Written : {rows:,} rows -> {out_path}")

print("\n All tables migrated to volume")

# COMMAND ----------

print("Files in gold volume:")
for f in dbutils.fs.ls(GOLD_VOLUME):
    print(f"  {f.name}")

print("\nAccess options:")
print("  Via table name : spark.table('senseofsensors_catalog.gold.GoldMaster')")
print("  Via volume path: spark.read.format('delta').load('/Volumes/senseofsensors_catalog/gold/gold_volume/GoldMaster')")

# COMMAND ----------

print("=" * 55)
print("  Gold Volume Migration Summary")
print("=" * 55)

for table in TABLES:
    out_path  = f"{GOLD_VOLUME}/{table}"
    out_table = f"{GOLD_SCHEMA}.{table}"

    vol_rows   = spark.read.format("delta").load(out_path).count()
    table_rows = spark.table(out_table).count()
    match      = "Good" if vol_rows == table_rows else "not good"

    print(f"  {match} {table:<30} Volume: {vol_rows:>10,}  Table: {table_rows:>10,}")

print()
print("Files in gold volume:")
for f in dbutils.fs.ls(GOLD_VOLUME):
    print(f"  {f.name}")