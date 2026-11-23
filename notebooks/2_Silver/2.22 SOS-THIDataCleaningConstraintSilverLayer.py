# Databricks notebook source
# Databricks notebook source
#
# # SOS THI — Silver Cleaning
#
# Reads `bronze_volume/TemperatureHumidityIndex/` (CSV, semicolon-delimited, European decimal comma)
# and writes cleaned Delta files to `silver_volume/THI`.
#
# Key differences from ReferenceData:
# - Source is CSV, not Parquet. Semicolon delimiter, comma decimal mark.
# - Two timestamp formats in one file (`yyyy/MM/dd HH:mm:ss` and `yyyy-MM-dd HH:mm:ss`).
# - `Intern` columns are frequently NULL by design. not errors.
# - Dedup key is `(HerdIdentifier, MeasurementDatetime)` — one measurement per herd per hour.

# Same principles as ReferenceData: bronze untouched, every row kept, problems warned

# Steps: 1 Read · 2 Load rules · 3 Rename · 4 Blank/trim ·
# 5 Identity · 6 Timestamps · 7 Numerics · 8 Row-level checks ·
# 9 Required check · 10 Dedup · 11 Combine warnings · 12 Audit · 13 Write · 14 Summary


# COMMAND ----------

# MAGIC %md
# MAGIC **Private step.** The next cell loads the project's anonymisation helper, which is kept in the private
# MAGIC Databricks workspace and is not part of this repository. It defines `anonymise(df, dataset)`, which replaces
# MAGIC farm IDs with `Farm_A`–`Farm_H`, pseudonymises animal and life-number IDs with a secret key, and drops
# MAGIC free-text fields that can contain identifiers. Everything from silver onwards is therefore anonymous.

# COMMAND ----------

# MAGIC %run "./0b SOS-AnonymisationHelper"

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("bronze_volume", "/Volumes/senseofsensors_catalog/bronze/bronze_volume")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG        = dbutils.widgets.get("catalog")
BRONZE_VOLUME  = dbutils.widgets.get("bronze_volume").rstrip("/")
SILVER_VOLUME  = dbutils.widgets.get("silver_volume").rstrip("/")
SILVER_SCHEMA  = f"{CATALOG}.silver"
DATASET        = "TemperatureHumidityIndex"
SRC_PATH       = f"{BRONZE_VOLUME}/TemperatureHumidityIndex/"
DATA_PATH      = f"{SILVER_VOLUME}/TemperatureHumidityIndex"
TARGET_TABLE   = f"{SILVER_SCHEMA}.TemperatureHumidityIndex"

_warn_exprs = []
def add_warn(expr): _warn_exprs.append(expr)
def pp(s): return dict(kv.split("=",1) for kv in (s or "").split(";") if "=" in kv)

# COMMAND ----------

# Step 1 — Read bronze CSV (read-only)
# Semicolon delimiter; header present. All values read as strings — casting happens in later steps.

bronze = (spark.read
          .option("header", True)
          .option("sep", ";")
          .option("inferSchema", False)   # keep everything as string; we cast explicitly
          .option("encoding", "UTF-8")
          .csv(SRC_PATH))
BRONZE_ROWS = bronze.count()
print(f"bronze rows: {BRONZE_ROWS:,}  columns: {len(bronze.columns)}")
print("columns:", bronze.columns)
display(bronze.limit(10))

# COMMAND ----------

# Step 2 — Load rule tables

rules       = (spark.table(f"{SILVER_SCHEMA}.Rulebook")
               .where(F.col("dataset") == DATASET).orderBy("ordinal").collect())
constraints = [r.asDict() for r in spark.table(f"{SILVER_SCHEMA}.CleaningConstraints")
               .where(F.col("dataset") == DATASET).collect()]
if not rules:
    raise ValueError("No Rulebook rows for THI — run SOS_THI_Rulebook first.")

rename_map    = {r["sourceColumn"]: r["targetColumn"]
                 for r in rules if r["sourceColumn"] is not None}
target_order  = [r["targetColumn"] for r in rules]
type_map      = {r["targetColumn"]: r["dataType"] for r in rules}
strategy_map  = {r["targetColumn"]: (r["cleaningStrategy"] or "").split(";") for r in rules}
formats_map   = {r["targetColumn"]: (r["acceptedFormats"] or "").split("|")
                 for r in rules if r["acceptedFormats"]}
range_map     = {r["targetColumn"]: (r["minValue"], r["maxValue"])
                 for r in rules if r["minValue"] is not None or r["maxValue"] is not None}
required_cols = [r["targetColumn"] for r in rules if r["isRequired"]]
by_type = {}
for c in constraints:
    by_type.setdefault(c["constraintType"], []).append(c)

ts_cols    = [t for t, dt in type_map.items() if dt == "timestamp"]
int_cols   = [t for t, dt in type_map.items() if dt == "int"]
double_cols= [t for t, dt in type_map.items() if dt == "double"]
print(f"{len(rename_map)} cols | timestamps: {len(ts_cols)} ints: {len(int_cols)} doubles: {len(double_cols)}")

# COMMAND ----------

# Step 3 — Rename

df = bronze
for src, tgt in rename_map.items():
    if src in df.columns:
        df = df.withColumnRenamed(src, tgt)
df = df.select([c for c in target_order if c in df.columns])
print("columns after rename:", df.columns)


# COMMAND ----------

# Step 4 — Blank / trim strings

BLANK_TOKENS = ["","null","NULL","Null","NA","N/A","NaN","nan","-","."]
for c, dt in df.dtypes:
    if dt == "string":
        t = F.trim(F.col(c))
        df = df.withColumn(c, F.when(t.isin(BLANK_TOKENS), None).otherwise(t))
print("trim + blankToNull applied")


# COMMAND ----------

# Step 5 — Identity: HerdIdentifier (int cast + reject placeholder 0)

if "HerdIdentifier" in df.columns:
    df = df.withColumn("HerdIdentifier",
        F.regexp_replace(F.trim(F.col("HerdIdentifier")), r"\.0$", ""))
    bad = F.col("HerdIdentifier").isNotNull() & F.col("HerdIdentifier").isin(
        ["0", "00", "000", "NA", "N/A", "None", "NONE", "NULL", "null"])
    add_warn(F.when(bad, F.lit("invalidIdentifier:HerdIdentifier")))
    df = df.withColumn("HerdIdentifier", F.when(bad, None).otherwise(F.col("HerdIdentifier")))


# COMMAND ----------

# Step 6 — Timestamp parsing
# Two formats in the same file. Both tried; first match wins.
# | Warning | Meaning | Action |
# |---|---|---|
# | `invalidDateFormat:MeasurementDatetime` | Neither format matched | set to NULL |
# | `futureDate:MeasurementDatetime` | Timestamp after today | kept |
# | `oldDate:MeasurementDatetime` | Before 2000-01-01 | set to NULL |

MIN_TS = F.lit("2000-01-01").cast("timestamp")

if "MeasurementDatetime" in df.columns:
    fmts = formats_map.get("MeasurementDatetime", [
        "yyyy/MM/dd HH:mm:ss",
        "yyyy/MM/dd H:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd H:mm:ss",
    ])
    raw_val = F.col("MeasurementDatetime")
    parsed  = F.coalesce(*[F.try_to_timestamp(raw_val, F.lit(fmt)) for fmt in fmts])

    present     = raw_val.isNotNull()
    unparseable = present & parsed.isNull()
    future_ts   = present & parsed.isNotNull() & (parsed > F.current_timestamp())
    old_ts      = present & parsed.isNotNull() & (parsed < MIN_TS)

    add_warn(F.when(unparseable, F.lit("invalidDateFormat:MeasurementDatetime (set to null)")))
    add_warn(F.when(future_ts,   F.lit("futureDate:MeasurementDatetime (kept)")))
    add_warn(F.when(old_ts,      F.lit("oldDate:MeasurementDatetime (before 2000; set to null)")))

    df = df.withColumn("MeasurementDatetime",
                       F.when(unparseable | old_ts, None).otherwise(parsed))
    print("timestamp parsed")


# COMMAND ----------

# Step 7 — Numeric cleaning (comma → dot, cast, range check)
# Out-of-range values are nulled and warned.

def prep_num(col): return F.regexp_replace(F.col(col).cast("string"), ",", ".")

for col in int_cols:
    if col not in df.columns: continue
    df = df.withColumn(col, prep_num(col).cast("int"))

for col in double_cols:
    if col not in df.columns: continue
    df = df.withColumn(col, prep_num(col).cast("double"))
    lo, hi = range_map.get(col, (None, None))
    if lo is not None or hi is not None:
        bad = F.col(col).isNotNull() & (
            (F.col(col) < F.lit(lo) if lo is not None else F.lit(False)) |
            (F.col(col) > F.lit(hi) if hi is not None else F.lit(False))
        )
        add_warn(F.when(bad,
                        F.lit(f"outOfExpectedRange:{col} (nulled; expected {lo}–{hi})")))
        df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))

# THI formula check: THI = (1.8*T + 32) - (0.55 - 0.0055*RH) * (1.8*T - 26), tolerance ±0.5
def thi_formula(t_col, rh_col):
    t  = F.col(t_col)
    rh = F.col(rh_col)
    return (1.8 * t + 32) - (0.55 - 0.0055 * rh) * (1.8 * t - 26)

for thi_col, t_col, rh_col in [
    ("THIExternal", "TempExternalCelsius", "HumidityExternalPct"),
    ("THIInternal", "TempInternalCelsius", "HumidityInternalPct"),
]:
    if all(c in df.columns for c in [thi_col, t_col, rh_col]):
        expected = thi_formula(t_col, rh_col)
        mismatch = (F.col(thi_col).isNotNull() &
                    F.col(t_col).isNotNull() &
                    F.col(rh_col).isNotNull() &
                    (F.abs(F.col(thi_col) - expected) > 0.5))
        add_warn(F.when(mismatch,
                        F.lit(f"THIFormulaMismatch:{thi_col} (value kept; deviation > 0.5)")))

print("numeric cleaning + THI formula check done")



# COMMAND ----------

# Step 8 — Derived columns
# MeasurementDate, MeasurementHour, THIExternalStatus, THIInternalStatus

if "MeasurementDatetime" in df.columns:
    df = df.withColumn("MeasurementDate", F.to_date(F.col("MeasurementDatetime")))
    df = df.withColumn("MeasurementHour", F.hour(F.col("MeasurementDatetime")))

def thi_status(col):
    return (F.when(F.col(col).isNull(),        None)
             .when(F.col(col) < 50,            F.lit("NoStress"))
             .when(F.col(col) < 72,            F.lit("Mild"))
             .when(F.col(col) < 80,            F.lit("HeatStress"))
             .otherwise(                        F.lit("SevereHeatStress")))

if "THIExternal" in df.columns:
    df = df.withColumn("THIExternalStatus", thi_status("THIExternal"))
if "THIInternal" in df.columns:
    df = df.withColumn("THIInternalStatus", thi_status("THIInternal"))

print("Derived columns added: MeasurementDate, MeasurementHour, THIExternalStatus, THIInternalStatus")
display(df.select("MeasurementDatetime", "MeasurementDate", "MeasurementHour",
                  "THIExternal", "THIExternalStatus",
                  "THIInternal", "THIInternalStatus").limit(10))

# COMMAND ----------

# Step 9 — Row-level check: at least one THI value present
# Rows where both `THIIntern` and `THIExtern` are NULL are kept but warned.

#code removed


# COMMAND ----------

# Step 10 — Required field check

for col in required_cols:
    if col in df.columns:
        add_warn(F.when(F.col(col).isNull(), F.lit(f"missingRequired:{col}")))


# COMMAND ----------

# Step 11 — Deduplication (exact match on HerdIdentifier + MeasurementDatetime)
# Removes exact duplicates; kept row receives a `duplicatesRemoved` warning.

dk = next((r for r in constraints if r["constraintType"] == "dedupExact"), None)
keys = [k for k in (pp(dk["params"]).get("keys","").split("|") if dk else []) if k in df.columns]
ROWS_BEFORE_DEDUP = df.count()
DEDUP_REMOVED = 0

if keys:
    data_cols = [c for c in df.columns if c not in keys]
    non_null_score = sum(F.col(c).isNotNull().cast("int") for c in data_cols)
    has_all_keys = reduce(lambda a, k: a & F.col(k).isNotNull(), keys, F.lit(True))

    complete   = df.where(has_all_keys)
    incomplete = df.where(~has_all_keys)

    w_part = Window.partitionBy(*keys)
    w_rank = Window.partitionBy(*keys).orderBy(F.desc(non_null_score))

    complete = (complete
                .withColumn("_grp", F.count("*").over(w_part))
                .withColumn("_rank", F.row_number().over(w_rank))
                .withColumn("_dup_warn",
                            F.when((F.col("_grp") > 1) & (F.col("_rank") == 1),
                                   F.concat(F.lit("duplicatesRemoved:"),
                                            (F.col("_grp")-1).cast("string"),
                                            F.lit(":keys="),
                                            F.concat_ws("|", *[F.col(k).cast("string") for k in keys])))))

    DEDUP_REMOVED = complete.where(F.col("_rank") > 1).count()
    kept = complete.where(F.col("_rank") == 1).drop("_grp","_rank")
    incomplete = incomplete.withColumn("_dup_warn", F.lit(None).cast("string"))
    df = kept.unionByName(incomplete)
    add_warn(F.col("_dup_warn"))
    print(f"Dedup on {keys}: removed {DEDUP_REMOVED:,}; {df.count():,} rows kept")
else:
    df = df.withColumn("_dup_warn", F.lit(None).cast("string"))
    print("No dedup keys — skipped")


# COMMAND ----------

# Step 12 — Combine warnings

df = df.withColumn(
    "Warnings",
    F.array_distinct(F.filter(F.array(*_warn_exprs), lambda x: x.isNotNull()))
    if _warn_exprs else F.array().cast("array<string>")
)
df = df.withColumn("WarningCount", F.size("Warnings"))
df = df.drop(*[c for c in ["_dup_warn"] if c in df.columns])

_temp_warn_cols = [c for c in df.columns if c.startswith("_w")]
df = df.withColumn(
    "Warnings",
    F.array_distinct(F.filter(F.array(*_warn_exprs), lambda x: x.isNotNull()))
    if _warn_exprs else F.array().cast("array<string>")
)
df = df.withColumn("WarningCount", F.size("Warnings"))
df = df.drop(*_temp_warn_cols, "_dup_warn")


# COMMAND ----------

# Step 13 — Audit columns

df = (df.withColumn("SourcePath",        F.lit(SRC_PATH))
        .withColumn("SilverProcessedAt", F.current_timestamp()))


# COMMAND ----------

# Step 14 — Write Delta to silver volume

df = anonymise(df, "TemperatureHumidityIndex")
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(DATA_PATH)
SILVER_ROWS = spark.read.format("delta").load(DATA_PATH).count()
print(f"WROTE {SILVER_ROWS:,} rows  ->  {DATA_PATH}")
try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} USING DELTA LOCATION '{DATA_PATH}'")
    print("Table registered:", TARGET_TABLE)
except Exception as e:
    print(f"Table registration skipped. Read with spark.read.format('delta').load('{DATA_PATH}')\n{e}")

# COMMAND ----------

# Step 15 — Run summary

silver = spark.read.format("delta").load(DATA_PATH)
wexp   = silver.select(F.explode("Warnings").alias("w"))
def cnt(pref): return wexp.where(F.col("w").startswith(pref)).count()

ROWS_WITH_WARN = silver.where(F.col("WarningCount") > 0).count()
summary = [
    ("bronzeRows",                      BRONZE_ROWS),
    ("exactDuplicatesRemoved",          DEDUP_REMOVED),
    ("silverRows",                      SILVER_ROWS),
    ("rowsClean",                       SILVER_ROWS - ROWS_WITH_WARN),
    ("rowsWithWarnings",                ROWS_WITH_WARN),
    ("invalidDateFormat (nulled)",      cnt("invalidDateFormat")),
    ("futureDate (kept)",               cnt("futureDate")),
    ("oldDate (nulled)",                cnt("oldDate")),
    ("outOfExpectedRange (nulled)",     cnt("outOfExpectedRange")),
    ("invalidIdentifier (nulled)",      cnt("invalidIdentifier")),
    ("missingBothTHI (kept+warned)",    cnt("missingBothTHI")),
    ("missingRequired",                 cnt("missingRequired")),
    ("duplicatesRemovedWarnings",       cnt("duplicatesRemoved")),
    ("THIFormulaMismatch (kept+warned)",    cnt("THIFormulaMismatch")),
]

print("=" * 52)
print("  SOS THI — Silver Run Summary")
print("=" * 52)
for k, v in summary:
    print(f"  {k:<36} {v:>10,}")

display(spark.createDataFrame(summary, ["metric","count"]))

display(wexp.groupBy("w").count().orderBy(F.desc("count")))
display(silver.limit(20))