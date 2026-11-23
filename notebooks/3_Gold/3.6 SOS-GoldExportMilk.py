# Databricks notebook source
# MAGIC %md
# MAGIC # SOS — add milk recording to SOS24h
# MAGIC Run after `3.5` (the SOS24h/SOS2h export) and `3.40` (gold DailyMilkYield). It does **not** rebuild the
# MAGIC existing files; it only adds:
# MAGIC
# MAGIC ```
# MAGIC SOS24h/
# MAGIC ├── DataDictionarySOS24h.csv   ← MilkDay rows added (replaced on re-run)
# MAGIC ├── ReadMe.txt                 ← "Milk recording" section added (replaced on re-run)
# MAGIC └── Farm_X/MilkDay.csv         ← one row per cow per milk-recording day (only farms with milk data)
# MAGIC ```
# MAGIC `MilkDay` links to `CowDay` and `Lactation` with `CowID` + `EventDay` (`CalvingDate` ≈ `LactationStart`).
# MAGIC Milk is daily only, so SOS2h is unchanged.

# COMMAND ----------

dbutils.widgets.text("Catalog", "senseofsensors_catalog")
dbutils.widgets.dropdown("OutputFormat", "CSV", ["CSV", "Parquet"])
CATALOG = dbutils.widgets.get("Catalog")
OUT_FMT = dbutils.widgets.get("OutputFormat")
EXT     = ".csv" if OUT_FMT == "CSV" else ".parquet"

GOLD   = f"/Volumes/{CATALOG}/gold/gold_volume"
ROOT   = f"{GOLD}/SOS24h"
DICT   = f"{ROOT}/DataDictionarySOS24h{EXT}"
README = f"{ROOT}/ReadMe.txt"
SECTION = "Milk recording (MilkDay)"

import datetime
from pyspark.sql import functions as F

milk = spark.read.format("delta").load(f"{GOLD}/DailyMilkYield")
FARMS = sorted(r[0] for r in milk.select("HerdIdentifier").distinct().collect())
print("Farms with milk data:", FARMS)

# COMMAND ----------

COLS = ["CowID", "HerdIdentifier", "EventDay", "LactationNumber", "CalvingDate", "DaysInMilk",
        "PreviousDryOffDate", "DryPeriodDays", "DailyMilkKg", "MilkFatPct", "MilkProteinPct", "MilkRecords",
        "RepeatedDayRecord", "ZeroMilkRecorded", "CalvingDateConflict", "HasSensorData", "CalvingDateMatchesGold"]
NEVER_EXPORT = {"AnimalIdentifier", "LifeNumber", "LifeNumberDigits", "BestLifeNumberDigits", "HerdName",
                "Warnings", "WarningCount", "SilverProcessedAt"}

milk_day = (milk.withColumn("CowID", F.col("LifeNumberDigits").cast("string"))
                .withColumn("PreviousDryOffDate", F.to_date("PreviousDryOffDate"))
                .select(*COLS))
leaked = set(milk_day.columns) & NEVER_EXPORT
assert not leaked, f"identifying columns would be exported: {leaked}"

DESC = {
    "CowID": ("", "Individual cow identifier (hashed); same CowID as in CowDay, Lactation and SOS2h"),
    "HerdIdentifier": ("", "Pseudonymised farm code"),
    "EventDay": ("date", "Calendar day of the milk record (yyyy-MM-dd)"),
    "LactationNumber": ("count", "Lactation number from the milking system (compare Parity)"),
    "CalvingDate": ("date", "Calving date of this lactation from the milking system (compare LactationStart)"),
    "DaysInMilk": ("d", "EventDay minus CalvingDate; milk records cover days 0-105 of lactation"),
    "PreviousDryOffDate": ("date", "Dry-off date before this calving (end of the previous lactation)"),
    "DryPeriodDays": ("d", "CalvingDate minus PreviousDryOffDate"),
    "DailyMilkKg": ("kg/d", "Milk yield for the day (repeated records averaged; 0 kg treated as missing)"),
    "MilkFatPct": ("%", "Milk fat content (0 or implausible values treated as missing)"),
    "MilkProteinPct": ("%", "Milk protein content (0 or implausible values treated as missing)"),
    "MilkRecords": ("count", "QC: number of source records for this cow-day (2+ = repeated record, averaged)"),
    "RepeatedDayRecord": ("boolean", "QC: the source stored this cow-day more than once"),
    "ZeroMilkRecorded": ("boolean", "QC: the source recorded 0 kg for this day (DailyMilkKg left empty)"),
    "CalvingDateConflict": ("boolean", "QC: repeated records disagreed on the calving date; the latest calving on "
                                       "or before the day was used"),
    "HasSensorData": ("boolean", "QC: CowDay has a row for this cow on this day"),
    "CalvingDateMatchesGold": ("boolean", "QC: CalvingDate equals the sensor-side LactationStart on this day"),
}

# COMMAND ----------

def exists(p):
    try:
        dbutils.fs.ls(p)
        return True
    except Exception:
        return False

def write_single(df, path, order):
    tmp = path + "__tmp"
    w = df.repartition(1).sortWithinPartitions(*order).write.mode("overwrite")
    if OUT_FMT == "CSV":
        w.option("header", True).option("dateFormat", "yyyy-MM-dd").csv(tmp)
    else:
        w.parquet(tmp)
    part = [f for f in dbutils.fs.ls(tmp) if f.name.startswith("part-")][0]
    if exists(path):
        dbutils.fs.rm(path)
    dbutils.fs.mv(part.path, path)
    dbutils.fs.rm(tmp, recurse=True)
    return part.size

if not exists(ROOT):
    raise ValueError(f"{ROOT} not found — run 3.5 first.")

sizes = []
for farm in FARMS:
    if not exists(f"{ROOT}/{farm}"):
        print(f"  {farm}: no SOS24h folder (not exported by 3.5) — skipped")
        continue
    n = write_single(milk_day.where(F.col("HerdIdentifier") == farm), f"{ROOT}/{farm}/MilkDay{EXT}",
                     ["CowID", "EventDay"])
    sizes.append(f"  {farm}: MilkDay {n/1e6:,.2f} MB")
    print(sizes[-1])

# COMMAND ----------

# Data dictionary: replace the MilkDay rows
agg = milk_day.agg(F.count(F.lit(1)).alias("__n"),
                   *[F.count(F.col(c)).alias(f"nn__{c}") for c in COLS],
                   *[F.min(c).cast("string").alias(f"mn__{c}") for c, t in milk_day.dtypes if t in ("int", "bigint", "double", "date")],
                   *[F.max(c).cast("string").alias(f"mx__{c}") for c, t in milk_day.dtypes if t in ("int", "bigint", "double", "date")]
                   ).first().asDict()
rows = [("MilkDay", c, t, DESC[c][0], DESC[c][1], round(100 * agg[f"nn__{c}"] / max(agg["__n"], 1), 1),
         agg.get(f"mn__{c}") or "", agg.get(f"mx__{c}") or "") for c, t in milk_day.dtypes]
new_rows = spark.createDataFrame(rows, ["File", "Column", "Type", "Unit", "Description", "PctNonNull", "Min", "Max"])

if OUT_FMT == "CSV":
    old = spark.read.option("header", True).csv(DICT)
else:
    old = spark.read.parquet(DICT)
old = old.where(F.col("File") != "MilkDay")
merged = old.unionByName(new_rows.select(*[F.col(c).cast(old.schema[c].dataType) for c in old.columns])).cache()
merged.count()                      # read fully before overwriting the same file
write_single(merged, DICT, ["File"])
merged.unpersist()
print(f"Data dictionary updated: {len(rows)} MilkDay rows")

# COMMAND ----------

# ReadMe: replace the milk section
s = milk.agg(F.min("EventDay"), F.max("EventDay"), F.countDistinct("LifeNumberDigits"), F.count("*"),
             F.avg(F.col("HasSensorData").cast("int")), F.avg(F.col("MilkFatPct").isNotNull().cast("int"))).first()
section = [SECTION, "-" * len(SECTION),
           f"Added: {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M} UTC",
           f"Farms with milk data: {', '.join(FARMS)} (the other farms have no MilkDay file).",
           f"{s[3]:,} cow-days from {s[2]:,} cows, {s[0]} to {s[1]}; days 0-105 of each lactation only.",
           f"{s[4]:.0%} of milk cow-days also have a CowDay row (HasSensorData); milk recording starts before the "
           "sensor data, so early days have milk only.",
           f"Fat and protein are present on {s[5]:.0%} of cow-days (sparse on some farms).",
           "Link: MilkDay.CowID + EventDay = CowDay.CowID + EventDay; CalvingDate corresponds to LactationStart.",
           "Repeated records for one cow-day were averaged (MilkRecords > 1); 0 kg is left empty (ZeroMilkRecorded).",
           ""] + sizes + [""]

txt = dbutils.fs.head(README, 10_000_000)
if SECTION in txt:
    txt = txt[:txt.index(SECTION)].rstrip() + "\n"
txt = txt.replace("Per farm: CowDay", "Per farm: MilkDay (milk recording, where available; see below), CowDay", 1) \
      if "MilkDay (milk recording" not in txt else txt
dbutils.fs.put(README, txt + "\n" + "\n".join(section), overwrite=True)
print("\n".join(section))
