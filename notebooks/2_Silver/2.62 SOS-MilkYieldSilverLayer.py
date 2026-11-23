# Databricks notebook source
# MAGIC %pip install openpyxl==3.1.2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("MinOverlap",    "0.3")   # share of a sheet's cows that must be found on the matched farm
dbutils.widgets.text("FarmOverride",  "")      # optional, e.g. "1=Farm_C;2=Farm_F" (sheet number = Farm)

CATALOG       = dbutils.widgets.get("catalog").strip()
MIN_OVERLAP   = float(dbutils.widgets.get("MinOverlap"))
OVERRIDE      = dict(p.split("=") for p in dbutils.widgets.get("FarmOverride").replace(" ", "").split(";") if "=" in p)
SILVER_SCHEMA = f"{CATALOG}.silver"
BRONZE_DIR    = f"/Volumes/{CATALOG}/bronze/bronze_volume/MilkYield"
SILVER_VOLUME = f"/Volumes/{CATALOG}/silver/silver_volume"
GOLD_VOLUME   = f"/Volumes/{CATALOG}/gold/gold_volume"
DATASET       = "MilkYield"
DATA_PATH     = f"{SILVER_VOLUME}/{DATASET}"
TARGET_TABLE  = f"{SILVER_SCHEMA}.{DATASET}"

# COMMAND ----------

# MAGIC %md
# MAGIC **Private step.** The next cell loads the project's anonymisation helper, which is kept in the private
# MAGIC Databricks workspace and is not part of this repository. It defines `anonymise(df, dataset)`, which replaces
# MAGIC farm IDs with `Farm_A`–`Farm_H`, pseudonymises animal and life-number IDs with a secret key, and drops
# MAGIC free-text fields that can contain identifiers. Everything from silver onwards is therefore anonymous.

# COMMAND ----------

# MAGIC %run "./0b SOS-AnonymisationHelper"

# COMMAND ----------

import pandas as pd
from functools import reduce
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Rules from the shared Rulebook. Column names in that table may be camelCase or PascalCase
# (it was created by an earlier rulebook version), so read every row case-insensitively.
rules = [{k.lower(): v for k, v in r.asDict().items()} for r in
         spark.table(f"{SILVER_SCHEMA}.Rulebook").where(F.col("Dataset") == DATASET).orderBy("Ordinal").collect()]
if not rules:
    raise ValueError("No MilkYield rows in the Rulebook — run 2.61 first.")
SRC2TGT    = {r["sourcecolumn"]: r["targetcolumn"] for r in rules if r["sourcecolumn"]}
TYPES      = {r["targetcolumn"]: r["datatype"] for r in rules}
RANGES     = {r["targetcolumn"]: (r["minvalue"], r["maxvalue"]) for r in rules
              if r["minvalue"] is not None or r["maxvalue"] is not None}
ZERO_NULL  = [r["targetcolumn"] for r in rules if "ZeroToNull" in (r["cleaningstrategy"] or "")]
FORMATS    = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd", "dd-MM-yyyy"]
BLANK      = ["", "NA", "N/A", "nan", "NaN", "NaT", "None", "null", "NULL", "-"]
print(f"Rulebook: {len(rules)} columns · ranges for {sorted(RANGES)} · zero→null for {ZERO_NULL}")

# Read the workbook (all values as text; typing happens below, by rulebook)
files = [f for f in dbutils.fs.ls(BRONZE_DIR) if f.name.lower().endswith(".xlsx")]
if len(files) != 1:
    raise ValueError(f"Expected exactly one .xlsx in {BRONZE_DIR}, found {[f.name for f in files]}")
frames = []
for i, (_, sheet) in enumerate(pd.read_excel(files[0].path.replace("dbfs:", ""), sheet_name=None, dtype=str).items(), 1):
    sheet.columns = [c.strip() for c in sheet.columns]
    sheet = sheet[[c for c in sheet.columns if c in SRC2TGT]].rename(columns=SRC2TGT)
    sheet["SheetNo"] = i                      # sheet names (farm names) are never kept
    frames.append(sheet)
pdf = pd.concat(frames, ignore_index=True)
pdf = pdf.astype(object).where(pdf.notna(), None)
src_cols = [c for c in pdf.columns if c != "SheetNo"]
raw = spark.createDataFrame(pdf, StructType([StructField(c, StringType()) for c in src_cols] +
                                            [StructField("SheetNo", IntegerType())]))
print(f"Bronze rows: {raw.count():,} from {len(frames)} sheets")

# COMMAND ----------

# Step 1 — trim, blanks to null, drop exact duplicates
for c in src_cols:
    raw = raw.withColumn(c, F.when(F.trim(c).isin(BLANK), None).otherwise(F.trim(c)))
raw = raw.withColumn("LifeNumberDigits", F.upper("LifeNumberDigits"))

before = raw.count()
raw = raw.dropDuplicates()
EXACT_DUPS = before - raw.count()
print(f"Exact duplicate rows removed: {EXACT_DUPS:,}")

# Step 2 — type every column as declared in the Rulebook; remember what failed to parse
def parse(c, t):
    if t == "date":
        return F.to_date(F.coalesce(*[F.to_timestamp(F.col(c), f) for f in FORMATS]))
    if t == "timestamp":
        return F.coalesce(*[F.to_timestamp(F.col(c), f) for f in FORMATS])
    if t == "int":
        return F.round(F.col(c).cast("double")).cast("int")
    if t == "double":
        return F.col(c).cast("double")
    return F.col(c)

df = raw
for c in src_cols:
    df = (df.withColumn(f"_raw_{c}", F.col(c))
            .withColumn(c, parse(c, TYPES.get(c, "string"))))

warn = []   # (condition, message)
for c in src_cols:
    warn.append((F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull(), f"InvalidValue:{c} (set to null)"))

# Step 3 — zero means "not measured" for the content columns
for c in ZERO_NULL:
    df = df.withColumn(f"_zero_{c}", F.col(c) == 0).withColumn(c, F.when(F.col(c) == 0, None).otherwise(F.col(c)))
    warn.append((F.col(f"_zero_{c}"), f"ZeroAsMissing:{c} (set to null)"))

# Step 4 — derived columns and cross-field checks
df = df.withColumn("DryPeriodDays", F.datediff("CalvingDate", F.to_date("PreviousDryOffDate")))
rep = Window.partitionBy("SheetNo", "LifeNumberDigits", "EventDay")
df = df.withColumn("_rep", F.when(F.col("LifeNumberDigits").isNotNull(), F.count(F.lit(1)).over(rep)).otherwise(1))

warn += [
    (F.col("AnimalIdentifier").isNull(), "MissingRequired:AnimalIdentifier"),
    (F.col("EventDay").isNull(), "MissingRequired:EventDay"),
    (F.col("LifeNumberDigits").isNull(), "MissingLifeNumber:LifeNumberDigits (not linkable; left out of gold)"),
    (F.col("DailyMilkKg") == 0, "ZeroValue:DailyMilkKg (kept; missing in gold)"),
    (F.col("DaysInMilk") != F.datediff("EventDay", "CalvingDate"), "DaysInMilkMismatch:DaysInMilk (kept)"),
    (F.col("PreviousDryOffDate").isNotNull() & (F.to_date("PreviousDryOffDate") >= F.col("CalvingDate")),
     "DryOffAfterCalving:PreviousDryOffDate (kept)"),
    (F.col("_rep") > 1, "RepeatedDayRecord:EventDay (kept; averaged in gold)"),
]
for c, (lo, hi) in RANGES.items():
    below = (F.col(c) < lo) if lo is not None else F.lit(False)
    above = (F.col(c) > hi) if hi is not None else F.lit(False)
    cond = F.col(c).isNotNull() & (below | above)
    if c == "DailyMilkKg":
        cond = cond & (F.col(c) != 0)          # zero has its own warning
    warn.append((cond, f"OutOfExpectedRange:{c} (kept)"))

df = (df.withColumn("Warnings", F.filter(F.array(*[F.when(cond, F.lit(msg)) for cond, msg in warn]),
                                         lambda x: x.isNotNull()))
        .withColumn("WarningCount", F.size("Warnings"))
        .drop("_rep", *[c for c in df.columns if c.startswith("_raw_") or c.startswith("_zero_")]))

print(f"Rows: {df.count():,} · with warnings: {df.where('WarningCount > 0').count():,}")
display(df.select(F.explode("Warnings").alias("Warning")).groupBy("Warning").count().orderBy(F.desc("count")))

# COMMAND ----------

def known_cows():
    srcs = [(f"{SILVER_VOLUME}/ReferenceData", "LifeNumberDigits"),
            (f"{GOLD_VOLUME}/LactationWindows", "LifeNumberDigits"),
            (f"{GOLD_VOLUME}/GoldMaster", "BestLifeNumberDigits")]
    out = []
    for path, col in srcs:
        try:
            t = spark.read.format("delta").load(path)
        except Exception:
            print(f"  (not found, skipped: {path})")
            continue
        if col in t.columns:
            out.append(t.select("HerdIdentifier", F.col(col).alias("LN")).where(F.col("LN").isNotNull()))
    return reduce(lambda a, b: a.unionByName(b), out).distinct()

known = known_cows().cache()
milk_ln = (df.where(F.col("LifeNumberDigits").isNotNull())
             .select("SheetNo", anon_lifenumber(F.col("LifeNumberDigits")).alias("LN")).distinct())
sheet_cows = milk_ln.groupBy("SheetNo").agg(F.count("*").alias("SheetCows"))
match = (milk_ln.join(known, "LN").groupBy("SheetNo", "HerdIdentifier").agg(F.count("*").alias("SharedCows"))
                .join(sheet_cows, "SheetNo")
                .withColumn("Overlap", F.round(F.col("SharedCows") / F.col("SheetCows"), 3)))
best = (match.withColumn("_r", F.row_number().over(Window.partitionBy("SheetNo").orderBy(F.desc("SharedCows"))))
             .where("_r = 1").drop("_r").orderBy("SheetNo").collect())
display(match.orderBy("SheetNo", F.desc("SharedCows")))

assign = {}
for r in best:
    assign[str(r.SheetNo)] = r.HerdIdentifier
for s in sheet_cows.select("SheetNo").collect():
    assign.setdefault(str(s.SheetNo), None)
for s, farm in OVERRIDE.items():
    print(f"  Override: sheet {s} → {farm}")
    assign[s] = farm

problems = [f"sheet {s}: no matching farm" for s, f in assign.items() if f is None]
problems += [f"sheet {r.SheetNo}: best overlap {r.Overlap:.0%} with {r.HerdIdentifier} is below {MIN_OVERLAP:.0%}"
             for r in best if r.Overlap < MIN_OVERLAP and str(r.SheetNo) not in OVERRIDE]
farms_used = [f for f in assign.values() if f]
problems += [f"{f} was matched by more than one sheet" for f in set(farms_used) if farms_used.count(f) > 1]
problems += [f"unknown farm {f}" for f in farms_used if f not in _FARM_MAP.values()]
if problems:
    raise ValueError("Farm assignment needs a decision (set FarmOverride):\n  " + "\n  ".join(problems))
for s in sorted(assign, key=int):
    print(f"  Sheet {s} → {assign[s]}")

# Back to the real farm ID, because anonymise() needs it to hash cow numbers the same way as the other datasets
_REAL = {v: k for k, v in _FARM_MAP.items()}
herd_map = spark.createDataFrame([(int(s), _REAL[f]) for s, f in assign.items()], ["SheetNo", "HerdIdentifier"])
df = df.join(herd_map, "SheetNo").drop("SheetNo")
known.unpersist()

# COMMAND ----------

# Write (anonymised) — same pattern as the other silver datasets
ORDER = ["HerdIdentifier", "AnimalIdentifier", "LifeNumberDigits", "LactationNumber", "CalvingDate",
         "PreviousDryOffDate", "DryPeriodDays", "EventDay", "DaysInMilk", "DailyMilkKg", "MilkFatPct",
         "MilkProteinPct", "Warnings", "WarningCount"]
df = anonymise(df.select(*ORDER), DATASET).withColumn("SilverProcessedAt", F.current_timestamp())
df = df.coalesce(1).cache()
df.count()
(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DATA_PATH))
SILVER_ROWS = spark.read.format("delta").load(DATA_PATH).count()
print(f"WROTE {SILVER_ROWS:,} rows -> {DATA_PATH}")
try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} USING DELTA LOCATION '{DATA_PATH}'")
    print("Table registered:", TARGET_TABLE)
except Exception as e:
    print(f"Table registration skipped. Read with spark.read.format('delta').load('{DATA_PATH}')\n{e}")

# COMMAND ----------

silver = spark.read.format("delta").load(DATA_PATH)
display(silver.groupBy("HerdIdentifier").agg(
    F.count("*").alias("Rows"),
    F.countDistinct("LifeNumberDigits").alias("Cows"),
    F.min("EventDay").alias("FirstDay"), F.max("EventDay").alias("LastDay"),
    F.round(F.avg("DailyMilkKg"), 1).alias("MeanMilkKg"),
    F.round(F.avg(F.col("MilkFatPct").isNotNull().cast("int")) * 100, 1).alias("PctWithFat"),
    F.sum((F.col("WarningCount") > 0).cast("int")).alias("RowsWithWarnings")).orderBy("HerdIdentifier"))