# Databricks notebook source
# Databricks notebook source
# SOS ReferenceData — Silver Cleaning
# Reads bronze and the five rule tables. Writes new files to the silver volume.
# Warnings are added based on the changes made from the rulebook.

# Steps:
# 1 Read bronze · 2 Load rules · 3 Rename · 4 - · 5 Blank/trim strings ·
# 6 Identity cleaning · 7 Multi-format date parsing · 8 Numeric cleaning · 9 Code mapping ·
# 10 Notes cleaning + keyword flags · 11 Required check · 12 Cross-field checks ·
# 13 Duplicate flagging · 14 Combine warnings · 15 Audit · 16 Write · 17 Summary

# COMMAND ----------

# MAGIC %md
# MAGIC **Private step.** The next cell loads the project's anonymisation helper, which is kept in the private
# MAGIC Databricks workspace and is not part of this repository. It defines `anonymise(df, dataset)`, which replaces
# MAGIC farm IDs with `Farm_A`–`Farm_H`, pseudonymises animal and life-number IDs with a secret key, and drops
# MAGIC free-text fields that can contain identifiers. Everything from silver onwards is therefore anonymous.

# COMMAND ----------

# MAGIC %run "./0b SOS-AnonymisationHelper"

# COMMAND ----------

import re
from functools import reduce
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog",       "senseofsensors_catalog")
dbutils.widgets.text("bronze_volume", "/Volumes/senseofsensors_catalog/bronze/bronze_volume")
dbutils.widgets.text("silver_volume", "/Volumes/senseofsensors_catalog/silver/silver_volume")

CATALOG        = dbutils.widgets.get("catalog")
BRONZE_VOLUME  = dbutils.widgets.get("bronze_volume").rstrip("/")
SILVER_VOLUME  = dbutils.widgets.get("silver_volume").rstrip("/")
SILVER_SCHEMA  = f"{CATALOG}.silver"
DATASET        = "ReferenceData"
SRC_PATH       = f"{BRONZE_VOLUME}/{DATASET}/"
DATA_PATH      = f"{SILVER_VOLUME}/{DATASET}"
TARGET_TABLE   = f"{SILVER_SCHEMA}.{DATASET}"

_warn_exprs = []  # all warning expressions; combined at step 14
def add_warn(expr): _warn_exprs.append(expr)
def pp(s): return dict(kv.split("=",1) for kv in (s or "").split(";") if "=" in kv)

# COMMAND ----------

# Step 1 — Read and Display bronze

bronze = spark.read.parquet(SRC_PATH)
BRONZE_ROWS = bronze.count()
print(f"bronze rows: {BRONZE_ROWS:,}  columns: {len(bronze.columns)}")
display(bronze.limit(5))

# COMMAND ----------

# Step 2 — Load rule tables

rules       = (spark.table(f"{SILVER_SCHEMA}.Rulebook")
               .where(F.col("dataset") == DATASET).orderBy("ordinal").collect())
constraints = [r.asDict() for r in spark.table(f"{SILVER_SCHEMA}.CleaningConstraints")
               .where(F.col("dataset") == DATASET).collect()]
glossary    = [r.asDict() for r in spark.table(f"{SILVER_SCHEMA}.TranslationGlossary")
               .where(F.col("dataset") == DATASET).collect()]
code_maps   = [r.asDict() for r in spark.table(f"{SILVER_SCHEMA}.CodeMappings")
               .where(F.col("dataset") == DATASET).collect()]
if not rules:
    raise ValueError("No Rulebook rows — run the rulebook notebook first.")

rename_map   = {r["sourceColumn"]: r["targetColumn"] for r in rules}
target_order = [r["targetColumn"] for r in rules]
type_map     = {r["targetColumn"]: r["dataType"] for r in rules}
strategy_map = {r["targetColumn"]: (r["cleaningStrategy"] or "").split(";") for r in rules}
range_map    = {r["targetColumn"]: (r["minValue"], r["maxValue"])
                for r in rules if r["minValue"] is not None or r["maxValue"] is not None}
required_cols= [r["targetColumn"] for r in rules if r["isRequired"]]

by_type = {}
for c in constraints:
    by_type.setdefault(c["constraintType"],[]).append(c)

date_cols   = [t for t,dt in type_map.items() if dt=="date"]
int_cols    = [t for t,dt in type_map.items() if dt=="int"]
double_cols = [t for t,dt in type_map.items() if dt=="double"]
print(f"{len(rename_map)} cols | dates: {len(date_cols)} ints: {len(int_cols)} doubles: {len(double_cols)}")

# COMMAND ----------

# Step 3 — Rename (drop `_N` suffix)
df = bronze
# Rename by explicit name match
for src, tgt in rename_map.items():
    if src in df.columns:
        df = df.withColumnRenamed(src, tgt)

unmapped = [c for c in bronze.columns if c not in rename_map]
if unmapped:
    print("WARNING — bronze columns not in rulebook (dropped):", unmapped)

df = df.select([c for c in target_order if c in df.columns])
print("columns after rename:", len(df.columns))

# COMMAND ----------

# check: verify a known column has the right type of value
if "CalvingType" in df.columns and "Repro4Uterus" in df.columns:
    ct_sample = [r[0] for r in df.select("CalvingType").where(F.col("CalvingType").isNotNull()).limit(3).collect()]
    ru_sample = [r[0] for r in df.select("Repro4Uterus").where(F.col("Repro4Uterus").isNotNull()).limit(3).collect()]
    print(f"CalvingType sample (expect 1-5 codes): {ct_sample}")
    print(f"Repro4Uterus sample (expect S/A/I/V codes): {ru_sample}")
    if any("/" in str(v) for v in ru_sample):
        raise ValueError("COLUMN MISALIGNMENT DETECTED: Repro4Uterus contains dates."
                         "This is likely due to a column rename error in the rulebook notebook.")

# COMMAND ----------

# NOT USING THIS STEP AS IT CREATES SEVERAL UNNECESSARY COLOUMNS!
# Step 4 — no backup columns are retained in the silver layer.

# COMMAND ----------

# Step 5 — REMOVE Blank / trim strings
BLANK_TOKENS = ["", "null", "NULL", "Null", "NA", "N/A", "NaN", "nan", "-", " -", ".", "?", " "]
for row in by_type.get("blankToNull", []):
    p = pp(row["params"])
    if "tokens" in p:
        BLANK_TOKENS = p["tokens"].split("|")

for c, dt in df.dtypes:
    t = F.trim(F.col(c))
    df = df.withColumn(c, F.when(t.isin(BLANK_TOKENS), None).otherwise(t))
print("trim + blankToNull applied")

# COMMAND ----------

# Step 6a — Identity cleaning
# HerdIdentifier / AnimalId: trim, strip `.0` suffix (float to string like `"1234.0"`).
# LifeNumber: upper-case, collapse spaces - `"NL 123456789"` to `LifeNumberDigits` (`"123456789"`).
# Placeholder identifiers: `0`, `00`, `000` to give NULL + `invalidIdentifier` warning.

for col in ["HerdIdentifier","AnimalIdentifier"]:
    if col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"\.0$", ""))

if "LifeNumber" in df.columns:
    # Strip trailing
    ln_stripped = F.regexp_replace(F.trim(F.col("LifeNumber")), r"\*$", "")
    add_warn(F.when(
        F.col("LifeNumber").isNotNull() & F.col("LifeNumber").endswith("*"),
        F.lit("flaggedLifeNumber (asterisk stripped; tag was replaced)")
    ))
    ln = F.upper(F.regexp_replace(ln_stripped, r"\s+", " "))
    df = df.withColumn("LifeNumber", ln)
    df = df.withColumn("LifeNumberDigits", F.regexp_replace(F.col("LifeNumber"), r"\D", ""))
    add_warn(F.when(
        F.col("LifeNumber").isNotNull() & ~F.col("LifeNumber").rlike(r"^[A-Z]{2}\s\d+$"),
        F.lit("invalidLifeNumberFormat")
    ))

reject_vals = ["0","00","000"]
for row in by_type.get("rejectValues",[]):
    reject_vals = [v for v in pp(row["params"]).get("values","").split("|") if v]
for id_col in ["HerdIdentifier","AnimalIdentifier","LifeNumber"]:
    if id_col in df.columns:
        bad = F.col(id_col).isNotNull() & F.col(id_col).isin(reject_vals)
        add_warn(F.when(bad, F.lit(f"invalidIdentifier:{id_col}")))
        df = df.withColumn(id_col, F.when(bad, None).otherwise(F.col(id_col)))

display(df.select(*[c for c in ["HerdIdentifier","AnimalIdentifier","LifeNumber","LifeNumberDigits"] if c in df.columns]).limit(10))

# COMMAND ----------

# Step 6b — Build ClinicalObservationsOverflow
# Collects non-code / unexpected text from typed columns before they are nulled.
# Format: "SourceColumn:value; SourceColumn:value"

overflow_parts = []

# CalvingType: multi-code values ("2 en 3"), ambiguous ("2,1"), unknown code ("6")
if "CalvingType" in df.columns:
    bad_ct = F.col("CalvingType").isNotNull() & (
        F.col("CalvingType").rlike(r"(?i)\ben\b") |          # "2 en 3"
        F.col("CalvingType").rlike(r",\d") |                  # "2,1"
        ~F.col("CalvingType").isin(["1","2","3","4","5"])
    )
    overflow_parts.append(
        F.when(bad_ct, F.concat(F.lit("CalvingType:"), F.col("CalvingType"))))
    add_warn(F.when(bad_ct, F.lit("multiCodeValue:CalvingType (moved to overflow)")))
    df = df.withColumn("CalvingType", F.when(bad_ct, None).otherwise(F.col("CalvingType")))

# UterineTone: letter "A" after typo fixes
for col in ["Repro4UterineTone", "Repro8UterineTone"]:
    if col in df.columns:
        bad = F.col(col).isNotNull() & ~F.col(col).isin(["+", "-", "+/-", "-/+"])
        overflow_parts.append(
            F.when(bad, F.concat(F.lit(f"{col}:"), F.col(col))))
        add_warn(F.when(bad, F.lit(f"unknownCode:{col} (moved to overflow)")))
        df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))

# Repro8LeftOvary: eg "Tumor"
if "Repro8LeftOvary" in df.columns:
    bad = F.col("Repro8LeftOvary").isNotNull() & ~F.col("Repro8LeftOvary").isin(["A","C","I","S"])
    overflow_parts.append(
        F.when(bad, F.concat(F.lit("Repro8LeftOvary:"), F.col("Repro8LeftOvary"))))
    add_warn(F.when(bad, F.lit("unknownCode:Repro8LeftOvary (moved to overflow)")))
    df = df.withColumn("Repro8LeftOvary", F.when(bad, None).otherwise(F.col("Repro8LeftOvary")))

# CycleWeek8: non-numeric text ("Mogelijk vroege dracht", "9/Vroege dracht" etc.)
if "CycleWeek8" in df.columns:
    bad = F.col("CycleWeek8").isNotNull() & F.col("CycleWeek8").rlike(r"[a-zA-Z]")
    overflow_parts.append(
        F.when(bad, F.concat(F.lit("CycleWeek8:"),
               F.regexp_replace(F.col("CycleWeek8"), r"\*", ""))))
    df = df.withColumn("CycleWeek8",
        F.when(bad, F.regexp_extract(F.col("CycleWeek8"), r"(\d+)", 1))
         .otherwise(F.regexp_replace(F.col("CycleWeek8"), r"\*$", "")))

# IndividualAttentionNType: non-numeric codes; Excel serials (42xxx/43xxx); multi-codes
ATTENTION_TYPE_COLS = [
    "IndividualAttention1Type", "IndividualAttention2Type",
    "IndividualAttention3Type", "IndividualAttention4Type",
]
for col in ATTENTION_TYPE_COLS:
    if col not in df.columns:
        continue
    # Excel serials: 5-digit numbers >= eg 40000
    excel_serial = F.col(col).isNotNull() & F.col(col).rlike(r"^\d{5,}$") & (F.col(col).cast("int") >= 40000)
    # Non-single-digit-numeric: anything not purely 1-9
    non_code = F.col(col).isNotNull() & ~F.col(col).rlike(r"^[1-9]$")
    bad = excel_serial | non_code
    overflow_parts.append(
        F.when(bad, F.concat(F.lit(f"{col}:"), F.col(col))))
    add_warn(F.when(bad, F.lit(f"invalidAttentionType:{col} (moved to overflow)")))
    df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))

# KetosisAttention1MPR: free-text values
if "KetosisAttention1MPR" in df.columns:
    bad = F.col("KetosisAttention1MPR").isNotNull() & F.col("KetosisAttention1MPR").rlike(r"[a-zA-Z]")
    overflow_parts.append(
        F.when(bad, F.concat(F.lit("KetosisAttention1MPR:"), F.col("KetosisAttention1MPR"))))
    add_warn(F.when(bad, F.lit("invalidNumeric:KetosisAttention1MPR (moved to overflow)")))
    df = df.withColumn("KetosisAttention1MPR", F.when(bad, None).otherwise(F.col("KetosisAttention1MPR")))

# KetosisAttention2DIM: Excel serial + free text
if "KetosisAttention2DIM" in df.columns:
    bad_serial = F.col("KetosisAttention2DIM").isNotNull() & F.col("KetosisAttention2DIM").rlike(r"^\d{5,}$")
    bad_text   = F.col("KetosisAttention2DIM").isNotNull() & F.col("KetosisAttention2DIM").rlike(r"[a-zA-Z]")
    bad = bad_serial | bad_text
    overflow_parts.append(
        F.when(bad, F.concat(F.lit("KetosisAttention2DIM:"), F.col("KetosisAttention2DIM"))))
    add_warn(F.when(bad, F.lit("invalidNumeric:KetosisAttention2DIM (moved to overflow)")))
    df = df.withColumn("KetosisAttention2DIM", F.when(bad, None).otherwise(F.col("KetosisAttention2DIM")))

# Combine all overflow parts into one semicolon-separated column
if overflow_parts:
    combined = F.concat_ws("; ", F.array(*[F.coalesce(p, F.lit(None)) for p in overflow_parts]))
    df = df.withColumn("ClinicalObservationsOverflow",
        F.when(F.trim(combined) == "", None).otherwise(combined))
else:
    df = df.withColumn("ClinicalObservationsOverflow", F.lit(None).cast("string"))

print("ClinicalObservationsOverflow built")
display(df.select("ClinicalObservationsOverflow")
         .where(F.col("ClinicalObservationsOverflow").isNotNull()).limit(20))

# COMMAND ----------

# Step 6c — Code column normalisation and allowedValues

# Fixes typos found in the source data before code mapping and allowedValues:
# - Uppercase all code columns (catches `v` → `V`, `a` → `A`)
# - Strip leading backtick (`` `+ `` → `+`)
# - `++` → `+` (found a double-plus typo in UterineTone) 
# - `_` → NULL (placeholder in Involution)
# - `allowedValues` : values not in the declared set → NULL + `unknownCode` warning
 
# Columns with declared allowedValues in the rulebook
allowed_vals_map = {r["targetColumn"]: set(r["allowedValues"].split("|"))
                    for r in rules if r["allowedValues"]}

CODE_UPPER_COLS = [
    "CalvingType", "CalvingSignal", "CalfRemoval", "Afterbirth",
    "Repro4Uterus", "Repro4LeftOvary", "Repro4RightOvary", "Repro4UterineTone", "Repro4Involution",
    "Repro8Uterus", "Repro8LeftOvary", "Repro8RightOvary", "Repro8UterineTone", "Repro8Involution",
]

CODE_TYPO_FIXES = {
    "Repro4UterineTone": [(r"^`\+$", "+"), (r"^\+\+$", "+"), (r"^--$", "-")],
    "Repro8UterineTone": [(r"^\+\+$", "+"), (r"^--$", "-")],
    "Repro4Involution":  [(r"^_$", "-"), (r"^--$", "-")],
    "Repro8Involution":  [(r"^--$", "-")],
}

for col in CODE_UPPER_COLS:
    if col in df.columns:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))

for col, fixes in CODE_TYPO_FIXES.items():
    if col not in df.columns:
        continue
    for pattern, replacement in fixes:
        if replacement is None:
            bad = F.col(col).isNotNull() & F.col(col).rlike(pattern)
            add_warn(F.when(bad, F.lit(f"invalidCode:{col} (placeholder value set to null)")))
            df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))
        else:
            df = df.withColumn(col,
                F.when(F.col(col).isNotNull() & F.col(col).rlike(pattern), F.lit(replacement))
                 .otherwise(F.col(col)))

# allowedValues enforcement for string code columns
for col, allowed in allowed_vals_map.items():
    if col not in df.columns:
        continue
    if type_map.get(col) != "string":
        continue 
    bad = F.col(col).isNotNull() & ~F.col(col).isin(list(allowed))
    add_warn(F.when(bad, F.lit(f"unknownCode:{col} (value not in allowed set; set to null)")))
    df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))

print("Code normalisation done")

# COMMAND ----------

# Step 7 — Multi-format date parsing

# Tries formats in order
# Two-digit years: `yy ≤ 30 → 20yy`, `yy > 30 → 19yy`.

# | Warning | Meaning | Action |
# | `ambiguousDate` | Two-digit year was used | value kept |
# | `invalidDateFormat` | Nothing parsed | value set to NULL |
# | `futureDate` | Date after today | value kept (may be expected calving) |
# | `oldDate` | Date before 2000-01-01 | value kept |


FMTS_4DIGIT = [
    "d/M/yyyy", "dd/MM/yyyy", "d/MM/yyyy", "dd/M/yyyy",   # slash, European
    "d-M-yyyy", "dd-MM-yyyy", "d-MM-yyyy", "dd-M-yyyy",   # dash, European
    "yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMdd",                # ISO / reversed
    "d MMM yyyy", "dd MMM yyyy", "d-MMM-yyyy", "dd-MMM-yyyy",  # month-name
]
FMTS_2DIGIT = [            # tried only when 4-digit fails;
    "d/M/yy", "dd/MM/yy", "d/MM/yy", "dd/M/yy",
    "d-M-yy", "dd-MM-yy",
]

MIN_DATE = F.lit("2000-01-01").cast("date")
FUTURE_WARN_ONLY = {"ExpectedCalvingDate"}

spark.conf.set("spark.sql.ansi.enabled", "false") 

# checkpoint before the date loop to clear any accumulated plan depth from steps 1-6
df = df.cache()
df.count()   # force materialisation

cur_dtypes = dict(df.dtypes)
date_warn_cols = []   # track materialised warning column names to fold in step 14

for col in date_cols:
    if col not in df.columns:
        continue

    if cur_dtypes[col] in ("date", "timestamp"):
        # already typed — just cast, no format parsing needed
        df = df.withColumn(col, F.col(col).cast("date"))
        continue

    raw = F.col(col)

    # one coalesce() per group — flat plan, not nested when()-chains
    # try_to_date returns NULL on format mismatch (works on all DBR versions where it exists)
    parsed_4 = F.coalesce(*[F.try_to_date(raw, F.lit(fmt)) for fmt in FMTS_4DIGIT])
    parsed_2 = F.coalesce(*[F.try_to_date(raw, F.lit(fmt)) for fmt in FMTS_2DIGIT])
    parsed   = F.coalesce(parsed_4, parsed_2)

    present     = raw.isNotNull()
    used_2digit = present & parsed_4.isNull() & parsed_2.isNotNull()
    unparseable = present & parsed.isNull()
    future_d    = present & parsed.isNotNull() & (parsed > F.current_date())
    old_d       = present & parsed.isNotNull() & (parsed < MIN_DATE)
    ambiguous   = used_2digit & parsed.isNotNull() & (F.dayofmonth(parsed) <= 12)

    w_ambig  = f"_wAmbig_{col}"
    w_badfmt = f"_wBadFmt_{col}"
    w_future = f"_wFuture_{col}"
    w_old    = f"_wOld_{col}"

    df = (df
          .withColumn(w_ambig,  F.when(ambiguous,   F.lit(f"ambiguousDate:{col} (day<=12; day/month uncertain)")))
          .withColumn(w_badfmt, F.when(unparseable,  F.lit(f"invalidDateFormat:{col} (set to null)")))
          .withColumn(w_future, F.when(future_d,     F.lit(f"futureDate:{col} (kept)"))))
    date_warn_cols += [w_ambig, w_badfmt, w_future]

    if col not in FUTURE_WARN_ONLY:          # <-- THIS MUST BE INSIDE THE for loop
        df = df.withColumn(
            w_old, F.when(old_d, F.lit(f"oldDate:{col} (before 2000; kept)")))
        date_warn_cols.append(w_old)

    df = df.withColumn(col, F.when(unparseable, None).otherwise(parsed))

# cache and count
df = df.cache()
df.count()

# fold date warning columns & then drop them
for wc in date_warn_cols:
    add_warn(F.col(wc))

print(f"Warnings: {len(date_warn_cols)}")
display(df.select(*[F.count(F.col(c)).alias(c) for c in date_cols if c in df.columns]))

# COMMAND ----------

# 8a
# Special pre-processing before generic numeric loop

# BcsStartDryPeriod: "2 5" -> "2.5" (space-as-decimal typo); slash-date typos -> NULL; cap at 5
if "BcsStartDryPeriod" in df.columns:
    df = df.withColumn("BcsStartDryPeriod",
        F.regexp_replace(F.col("BcsStartDryPeriod"), r"^(\d)\s(\d)$", "$1.$2"))
    slash_date = F.col("BcsStartDryPeriod").isNotNull() & F.col("BcsStartDryPeriod").rlike(r"\d+/\d+/\d+")
    add_warn(F.when(slash_date, F.lit("invalidNumeric:BcsStartDryPeriod (slash-date typo -> null)")))
    df = df.withColumn("BcsStartDryPeriod", F.when(slash_date, None).otherwise(F.col("BcsStartDryPeriod")))

# LocoEndDryPeriod: "2/01/1900" slash-date typo -> NULL
if "LocoEndDryPeriod" in df.columns:
    slash_date = F.col("LocoEndDryPeriod").isNotNull() & F.col("LocoEndDryPeriod").rlike(r"\d+/\d+/\d+")
    add_warn(F.when(slash_date, F.lit("invalidNumeric:LocoEndDryPeriod (slash-date typo -> null)")))
    df = df.withColumn("LocoEndDryPeriod", F.when(slash_date, None).otherwise(F.col("LocoEndDryPeriod")))

# DimCalcium: negative Excel serials (-42760, -42765) -> NULL
if "DimCalcium" in df.columns:
    neg_serial = F.col("DimCalcium").isNotNull() & F.col("DimCalcium").rlike(r"^-\d{5,}$")
    add_warn(F.when(neg_serial, F.lit("invalidNumeric:DimCalcium (negative Excel serial -> null)")))
    df = df.withColumn("DimCalcium", F.when(neg_serial, None).otherwise(F.col("DimCalcium")))

# DimBhba1: date value "21/03/2017" -> NULL
if "DimBhba1" in df.columns:
    slash_date = F.col("DimBhba1").isNotNull() & F.col("DimBhba1").rlike(r"\d+/\d+/\d+")
    add_warn(F.when(slash_date, F.lit("invalidNumeric:DimBhba1 (date value -> null)")))
    df = df.withColumn("DimBhba1", F.when(slash_date, None).otherwise(F.col("DimBhba1")))

# DimBhba2: negative Excel serials (-353, -43199) + date value "28/03/2017" -> NULL
if "DimBhba2" in df.columns:
    neg_serial = F.col("DimBhba2").isNotNull() & F.col("DimBhba2").rlike(r"^-\d+$")
    slash_date = F.col("DimBhba2").isNotNull() & F.col("DimBhba2").rlike(r"\d+/\d+/\d+")
    bad = neg_serial | slash_date
    add_warn(F.when(bad, F.lit("invalidNumeric:DimBhba2 (garbage value -> null)")))
    df = df.withColumn("DimBhba2", F.when(bad, None).otherwise(F.col("DimBhba2")))

# DimAtMPRSampling: Excel serials (42834+) -> NULL
if "DimAtMPRSampling" in df.columns:
    excel_serial = F.col("DimAtMPRSampling").isNotNull() & \
                   F.col("DimAtMPRSampling").rlike(r"^\d{5,}$") & \
                   (F.col("DimAtMPRSampling").cast("int") >= 40000)
    add_warn(F.when(excel_serial, F.lit("invalidNumeric:DimAtMPRSampling (Excel serial -> null)")))
    df = df.withColumn("DimAtMPRSampling", F.when(excel_serial, None).otherwise(F.col("DimAtMPRSampling")))

# CalvingDurationHr: flag suspected clock-time entries (>6h with .15/.25/.30/.45 decimal)
if "CalvingDurationHr" in df.columns:
    df = df.withColumn("CalvingDurationHr",
        F.regexp_replace(F.col("CalvingDurationHr"), ",", "."))
    dur = F.col("CalvingDurationHr").cast("double")
    clock_pattern = F.col("CalvingDurationHr").rlike(r"\.(15|25|30|45)$")
    suspected = dur.isNotNull() & (dur > 6) & clock_pattern
    add_warn(F.when(suspected, F.lit("suspectedClockTime:CalvingDurationHr (value kept)")))

# LocoWeek4: "20" -> 2 (extra-zero typo); "3,00" -> 3
if "LocoWeek4" in df.columns:
    df = df.withColumn("LocoWeek4",
        F.when(F.trim(F.col("LocoWeek4")) == "20", F.lit("2"))
         .otherwise(F.regexp_replace(F.col("LocoWeek4"), ",", ".")))

# BcsWeek4: "+" and values that are non-numeric -> NULL
if "BcsWeek4" in df.columns:
    non_num = F.col("BcsWeek4").isNotNull() & ~F.col("BcsWeek4").rlike(r"^-?\d*\.?\d+$")
    add_warn(F.when(non_num, F.lit("invalidNumeric:BcsWeek4 (non-numeric -> null)")))
    df = df.withColumn("BcsWeek4", F.when(non_num, None).otherwise(F.col("BcsWeek4")))

# BcsWeek8: "2m5" -> 2.5; "+" and "-" -> NULL
if "BcsWeek8" in df.columns:
    df = df.withColumn("BcsWeek8",
        F.regexp_replace(F.col("BcsWeek8"), r"(?i)(\d)m(\d)", "$1.$2"))
    non_num = F.col("BcsWeek8").isNotNull() & ~F.col("BcsWeek8").rlike(r"^-?\d*\.?\d+$")
    add_warn(F.when(non_num, F.lit("invalidNumeric:BcsWeek8 (non-numeric -> null)")))
    df = df.withColumn("BcsWeek8", F.when(non_num, None).otherwise(F.col("BcsWeek8")))

# BcsStartDryPeriod: cap values above 5 at 5.0 (after cast in the double loop below)
# Handled in the generic double loop via range_map — but cap not null.
# Override: after the double loop, re-apply cap logic.
# (Flagged here so the warning fires before the value is changed)
if "BcsStartDryPeriod" in df.columns:
    too_high = F.col("BcsStartDryPeriod").isNotNull() & \
               F.col("BcsStartDryPeriod").cast("double").isNotNull() & \
               (F.col("BcsStartDryPeriod").cast("double") > 5)
    add_warn(F.when(too_high, F.lit("outOfExpectedRange:BcsStartDryPeriod (capped at 5)")))

print("Special pre-processing done")

# COMMAND ----------

# Step 8b — generic Numeric cleaning

# comma → dot.
# Text extraction: if an `int` column contains letters (e.g. `"2 en 3"`), NULL it + `invalidNumeric`.
# Convert to declared type failure → NULL + `invalidNumeric`.
# Range check: value outside rulebook range → NULL + `outOfExpectedRange`.

def prep_num(col): return F.regexp_replace(F.col(col).cast("string"),",",".")

for col in int_cols:
    if col not in df.columns: continue
    prepped = prep_num(col)
    if "numericExtract" in strategy_map.get(col,[]):
        # text like "2 en 3" in a numeric field: null the cleaned value and add a warning
        has_letters = prepped.rlike(r"[a-zA-Z]")
        add_warn(F.when(F.col(col).isNotNull() & has_letters,
                        F.lit(f"invalidNumeric:textInNumericField:{col} (nulled)")))
        df = df.withColumn(col, F.when(F.col(col).isNotNull() & has_letters, None)
                                  .otherwise(prepped.cast("int")))
    else:
        df = df.withColumn(col, prepped.cast("int"))
    lo, hi = range_map.get(col,(None,None))
    if lo is not None or hi is not None:
        bad = F.col(col).isNotNull() & (
            (F.col(col) < F.lit(lo) if lo is not None else F.lit(False)) |
            (F.col(col) > F.lit(hi) if hi is not None else F.lit(False))
        )
        add_warn(F.when(bad,
                        F.lit(f"outOfExpectedRange:{col} (nulled; expected {lo}–{hi})")))
        df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))

for col in double_cols:
    if col not in df.columns: continue
    df = df.withColumn(col, prep_num(col).cast("double"))
    lo, hi = range_map.get(col,(None,None))
    if lo is not None or hi is not None:
        bad = F.col(col).isNotNull() & (
            (F.col(col) < F.lit(lo) if lo is not None else F.lit(False)) |
            (F.col(col) > F.lit(hi) if hi is not None else F.lit(False))
        )
        add_warn(F.when(bad,
                        F.lit(f"outOfExpectedRange:{col} (nulled; expected {lo}–{hi})")))
        df = df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))

# Cap BcsStartDryPeriod at 5.0 (not null — value is preserved at boundary)
if "BcsStartDryPeriod" in df.columns:
    df = df.withColumn("BcsStartDryPeriod",
        F.when(F.col("BcsStartDryPeriod") > 5, F.lit(5.0))
         .otherwise(F.col("BcsStartDryPeriod")))

print("numeric cleaning done"); df.printSchema()

# COMMAND ----------

# Step 9 — Code mapping
# Maps known codes to English labels. Unknown codes produce `unknownCode` warning.

col_code_map = {}
for row in code_maps:
    col_code_map.setdefault(row["column"],{})[row["code"]] = row["label"]

for col, mapping in col_code_map.items():
    if col not in df.columns: continue
    label_col = f"{col}Label"
    expr = F.lit(None).cast("string")
    for code, label in mapping.items():
        expr = F.when(F.col(col) == code, F.lit(label)).otherwise(expr)
    df = df.withColumn(label_col, expr)
    add_warn(F.when(F.col(col).isNotNull() & F.col(label_col).isNull(),
                    F.lit(f"unknownCode:{col}")))

print("code labels added:", [f"{c}Label" for c in col_code_map if c in df.columns])

# COMMAND ----------

# Step 10 — Notes trim
 
if "Notes" in df.columns:
    df = df.withColumn("Notes", F.regexp_replace(F.trim(F.col("Notes")), r"\s+", " "))
    print("Notes trimmed and normalised. Keyword flags will be derived in gold.")

# COMMAND ----------

# Step 11 — Required field check (warning only)

for col in required_cols:
    if col in df.columns:
        add_warn(F.when(F.col(col).isNull(), F.lit(f"missingRequired:{col}")))

# COMMAND ----------

# Step 12 — Cross-field checks (warnings)

def has(*cols): return all(c in df.columns for c in cols)
 
for r in constraints:
    ct, p, cid = r["constraintType"], pp(r["params"]), r["constraintId"]
    if ct == "dateOrder":
        seq = [c.strip() for c in p.get("order","").split("<=") if c.strip()]
        if len(seq) >= 2 and has(*seq):
            for x, y in zip(seq, seq[1:]):
                bad = F.col(x).isNotNull() & F.col(y).isNotNull() & (F.col(x) > F.col(y))
                add_warn(F.when(bad, F.lit(f"dateSequenceError:{cid}:{x}>{y}")))
    elif ct == "dimConsistency":
        d, a, dim = p.get("date"), p.get("anchor"), p.get("dim")
        tol = int(p.get("tol","3"))
        if has(d, a, dim):
            bad = (F.col(d).isNotNull() & F.col(a).isNotNull() & F.col(dim).isNotNull() &
                   (F.abs(F.datediff(F.col(d), F.col(a)) - F.col(dim)) > tol))
            add_warn(F.when(bad, F.lit(f"dimMismatch:{cid}:{dim}")))
    elif ct == "datediffRange":
        a, b = p.get("a"), p.get("b"); amax = int(p.get("absMax","30"))
        if has(a, b):
            bad = (F.col(a).isNotNull() & F.col(b).isNotNull() &
                   (F.abs(F.datediff(F.col(a), F.col(b))) > amax))
            add_warn(F.when(bad, F.lit(f"expectedDateMismatch:{cid}")))

# COMMAND ----------

# Step 12b — Remove ghost rows
# Remove rows where ALL THREE identity columns (`HerdIdentifier`, `AnimalIdentifier`, `LifeNumber`) are NULL
 
GHOST_REMOVED = 0
id_cols = [c for c in ["HerdIdentifier","AnimalIdentifier","LifeNumber"] if c in df.columns]
if id_cols:
    all_null = reduce(lambda a, c: a & F.col(c).isNull(), id_cols, F.lit(True))
    before_ghost = df.count()
    df = df.where(~all_null)
    GHOST_REMOVED = before_ghost - df.count()
    print(f"Ghost rows removed (all identity columns null): {GHOST_REMOVED:,}")

# COMMAND ----------


# Step 13 — Deduplication (exact identity match on ALL keys → rows removed, surviving row with a warning)

# A row is removed only when every identity key (`HerdIdentifier`, `AnimalIdentifier`, `LifeNumber`) matches
# another row exactly. The row with the most non-null values
# across data columns is kept; ties broken by row order. The kept row receives a warning in its
# `Warnings` column: `duplicatesRemoved:<n>:keys=<values>` so the decision is traceable.
 
dk = next((r for r in constraints if r["constraintType"] == "dedupExact"), None)
keys = [k for k in (pp(dk["params"]).get("keys","").split("|") if dk else []) if k in df.columns]
ROWS_BEFORE_DEDUP = df.count()
DEDUP_REMOVED = 0

if keys:
    # Only dedup rows where ALL keys are non-null
    data_cols_for_completeness = [c for c in df.columns if c not in keys]
    non_null_score = sum(F.col(c).isNotNull().cast("int") for c in data_cols_for_completeness)

    has_all_keys = reduce(lambda a, k: a & F.col(k).isNotNull(), keys, F.lit(True))
    complete_rows   = df.where(has_all_keys)
    incomplete_rows = df.where(~has_all_keys)  # missing any key → never deduped

    # For the key partition: count group size and rank by completeness (most non-null first)
    w_part  = Window.partitionBy(*keys)
    w_rank  = Window.partitionBy(*keys).orderBy(F.desc(non_null_score))

    complete_rows = (complete_rows
                     .withColumn("_grp_size",  F.count("*").over(w_part))
                     .withColumn("_rank",       F.row_number().over(w_rank))
                     .withColumn("_dup_warn",
                                 F.when(
                                     (F.col("_grp_size") > 1) & (F.col("_rank") == 1),
                                     F.concat(
                                         F.lit("duplicatesRemoved:"),
                                         (F.col("_grp_size") - 1).cast("string"),
                                         F.lit(":keys="),
                                         F.concat_ws("|", *[F.col(k).cast("string") for k in keys])
                                     )
                                 )))

    # keep only most complete row per key partition, — discard the rest
    kept    = complete_rows.where(F.col("_rank") == 1)
    DEDUP_REMOVED = complete_rows.where(F.col("_rank") > 1).count()

    # add the dup warning to _warn_exprs AFTER materialising it as a real column
    kept = kept.drop("_grp_size","_rank")
    # union back with incomplete rows (no _dup_warn column there)
    incomplete_rows = incomplete_rows.withColumn("_dup_warn", F.lit(None).cast("string"))
    df = kept.unionByName(incomplete_rows)

    # fold _dup_warn into the warnings accumulator
    add_warn(F.col("_dup_warn"))
    print(f"Dedup on {keys}: removed {DEDUP_REMOVED:,} exact duplicates; {df.count():,} rows kept")
else:
    df = df.withColumn("_dup_warn", F.lit(None).cast("string"))
    print("No dedup keys found in constraints — skipped")

# COMMAND ----------


# Step 14 — Combine all warnings → `Warnings` + `WarningCount`

_temp_warn_cols = [c for c in df.columns if c.startswith("_w")]
df = df.withColumn(
    "Warnings",
    F.array_distinct(F.filter(F.array(*_warn_exprs), lambda x: x.isNotNull()))
    if _warn_exprs else F.array().cast("array<string>")
)
df = df.withColumn("WarningCount", F.size("Warnings"))

# Drop all temporary warning staging columns now that they're folded in
df = df.drop(*_temp_warn_cols, "_dup_warn")
display(df.select(F.explode("Warnings").alias("w")).groupBy("w").count().orderBy(F.desc("count")))

# COMMAND ----------

# Step 15 — Audit columns

df = (df.withColumn("SourcePath",        F.lit(SRC_PATH))
        .withColumn("SilverProcessedAt", F.current_timestamp()))

# COMMAND ----------

# Step 16 — Write Delta to silver volume (same path overwritten each run)

df = anonymise(df, "ReferenceData")
df = df.drop("_dup_warn", *[c for c in df.columns if c.startswith("_w")])  # safety net in case any leaked
df = df.coalesce(1)   # write a single part file — data is small enough
df = df.cache()   # flatten plan before write
df.count()
(df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(DATA_PATH))
SILVER_ROWS = spark.read.format("delta").load(DATA_PATH).count()
print(f"WROTE {SILVER_ROWS:,} rows  ->  {DATA_PATH}")
try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} USING DELTA LOCATION '{DATA_PATH}'")
    print("Table registered:", TARGET_TABLE)
except Exception as e:
    print(f"Table registration skipped. Read with spark.read.format('delta').load('{DATA_PATH}')\n{e}")

# COMMAND ----------

# Step 17 — Run summary

silver   = spark.read.format("delta").load(DATA_PATH)
wexp     = silver.select(F.explode("Warnings").alias("w"))
def cnt(pref): return wexp.where(F.col("w").startswith(pref)).count()

ROWS_WITH_WARN = silver.where(F.col("WarningCount") > 0).count()

summary = [
    ("bronzeRows",                          BRONZE_ROWS),
    ("ghostRowsRemoved",                    GHOST_REMOVED),
    ("exactDuplicatesRemoved",              DEDUP_REMOVED),
    ("silverRows",                          SILVER_ROWS),
    ("rowsClean",                           SILVER_ROWS - ROWS_WITH_WARN),
    ("rowsWithWarnings",                    ROWS_WITH_WARN),
    ("rowsWhereExactDupsWereRemoved",       cnt("duplicatesRemoved")),
    ("invalidDateFormat (nulled)",          cnt("invalidDateFormat")),
    ("ambiguousDate (kept+warned)",         cnt("ambiguousDate")),
    ("futureDate (kept+warned)",            cnt("futureDate")),
    ("oldDate (nulled)",                    cnt("oldDate")),
    ("outOfExpectedRange (nulled)",         cnt("outOfExpectedRange")),
    ("invalidNumeric (nulled)",             cnt("invalidNumeric")),
    ("invalidIdentifier (nulled)",          cnt("invalidIdentifier")),
    ("flaggedLifeNumber (asterisk stripped)",cnt("flaggedLifeNumber")),
    ("invalidLifeNumberFormat",             cnt("invalidLifeNumberFormat")),
    ("unknownCode (nulled)",                cnt("unknownCode")),
    ("invalidCode (placeholder nulled)",    cnt("invalidCode")),
    ("missingRequired",                     cnt("missingRequired")),
    ("dateSequenceError",                   cnt("dateSequenceError")),
    ("dimMismatch",                         cnt("dimMismatch")),
    ("expectedDateMismatch",                cnt("expectedDateMismatch")),
    ("multiCodeValue (moved to overflow)",      cnt("multiCodeValue")),
    ("suspectedClockTime (kept+warned)",        cnt("suspectedClockTime")),
    ("invalidAttentionType (moved to overflow)",cnt("invalidAttentionType")),
    ("approximateTime (kept+warned)",           cnt("approximateTime")),
]

print("=" * 55)
print("  SOS ReferenceData — Silver Run Summary")
print("=" * 55)
for k, v in summary:
    print(f"  {k:<38} {v:>10,}")

display(spark.createDataFrame(summary, ["metric","count"]))

# COMMAND

print("Top warnings:")
display(wexp.groupBy("w").count().orderBy(F.desc("count")))
print("Sample silver rows:")
display(silver.limit(20))

# COMMAND ----------

# Step 18 — Data Dictionary
# Generates a queryable `silver.DataDictionary` Delta table and a downloadable Excel file.
# One row per column in the final silver output, combining rulebook metadata with observed stats.

# Sheets in the Excel:
# 1. `Columns` — full column dictionary with type, cleaning strategy, warnings expected
# 2. `WarningTypes` — distinct warning prefixes seen in this run with counts
# 3. `RunSummary` — the metrics table from step 17

import pandas as pd

DD_TABLE  = f"{SILVER_SCHEMA}.DataDictionary"
DD_PATH   = f"{SILVER_VOLUME}/DataDictionary-{DATASET}.xlsx"

# build column-level metadata
rulebook_pdf = (spark.table(f"{SILVER_SCHEMA}.Rulebook")
                .where(F.col("dataset") == DATASET).orderBy("ordinal").toPandas())

silver_cols = silver.columns
silver_dtypes = dict(silver.dtypes)

col_rows = []
for _, rb in rulebook_pdf.iterrows():
    tgt = rb["targetColumn"]
    col_rows.append({
        "dataset":           DATASET,
        "ordinal":           int(rb["ordinal"]),
        "sourceColumn":      rb["sourceColumn"],
        "silverColumn":      tgt,
        "sparkType":         silver_dtypes.get(tgt),
        "semanticGroup":     rb["semanticGroup"],
        "isRequired":        str(rb["isRequired"]),
        "minValue":          str(rb["minValue"]) if rb["minValue"] is not None else None,
        "maxValue":          str(rb["maxValue"]) if rb["maxValue"] is not None else None,
        "canonicalUnit":     rb["canonicalUnit"],
        "allowedUnits":      rb["allowedUnits"],
        "cleaningStrategy":  rb["cleaningStrategy"],
        "expectedWarnings":  rb["warningType"],
        "description":       rb["description"],
    })
    # label column if a code mapping exists
    label_col = f"{tgt}Label"
    if label_col in silver_cols:
        col_rows.append({
            "dataset": DATASET, "ordinal": int(rb["ordinal"]) + 0.5,
            "sourceColumn": None, "silverColumn": label_col,
            "sparkType": silver_dtypes.get(label_col),
            "semanticGroup": rb["semanticGroup"],
            "isRequired": "False", "minValue": None, "maxValue": None,
            "canonicalUnit": None, "allowedUnits": None,
            "cleaningStrategy": "derivedFromCodeMappings", "expectedWarnings": "unknownCode",
            "description": f"English label derived from {tgt} via CodeMappings table.",
        })

# add metadata/flag columns not in rulebook
meta_cols = [c for c in silver_cols if c not in {r["silverColumn"] for r in col_rows}]
for mc in meta_cols:
    col_rows.append({
        "dataset": DATASET, "ordinal": 9999,
        "sourceColumn": None, "silverColumn": mc,
        "sparkType": silver_dtypes.get(mc), "semanticGroup": "metadata",
        "isRequired": "False", "minValue": None, "maxValue": None,
        "canonicalUnit": None, "allowedUnits": None,
        "cleaningStrategy": "systemGenerated", "expectedWarnings": None,
        "description": "System-generated metadata / derived column.",
    })

dd_pdf = pd.DataFrame(col_rows).sort_values(["ordinal"]).reset_index(drop=True)

# persist as Delta table
dd_spark = spark.createDataFrame(dd_pdf.astype(str).where(dd_pdf.notna(), None))
if spark.catalog.tableExists(DD_TABLE):
    spark.sql(f"DELETE FROM {DD_TABLE} WHERE dataset = '{DATASET}'")
    dd_spark.write.mode("append").option("mergeSchema","true").saveAsTable(DD_TABLE)
else:
    dd_spark.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(DD_TABLE)
print(f"DataDictionary: {len(dd_pdf)} rows -> {DD_TABLE}")

# export as 3-sheet Excel workbook 
# warn_counts_pdf = (wexp.groupBy("w").count().orderBy(F.desc("count")).toPandas()
#                    .rename(columns={"w":"warningType","count":"occurrences"}))
# summary_pdf = pd.DataFrame(summary, columns=["metric","count"])

# tmp_dd = "/tmp/SOS-RefData-DataDictionary.xlsx"
# with pd.ExcelWriter(tmp_dd) as xl:
#     dd_pdf.drop(columns=["dataset"]).to_excel(xl, sheet_name="Columns", index=False)
#     warn_counts_pdf.to_excel(xl, sheet_name="WarningTypes", index=False)
#     summary_pdf.to_excel(xl, sheet_name="RunSummary", index=False)

# from openpyxl import load_workbook
# from openpyxl.styles import PatternFill, Font
# wb_dd = load_workbook(tmp_dd)
# hdr_fill = PatternFill("solid", fgColor="1F4E79"); hdr_font = Font(color="FFFFFF", bold=True)
# for ws in wb_dd.worksheets:
#     for cell in ws[1]: cell.fill = hdr_fill; cell.font = hdr_font
#     for col in ws.columns:
#         w = max((len(str(cell.value)) if cell.value else 0) for cell in col)
#         ws.column_dimensions[col[0].column_letter].width = min(max(w+2, 12), 70)
#     ws.freeze_panes = "A2"; ws.auto_filter.ref = ws.dimensions
# wb_dd.save(tmp_dd)

# try:
#    dbutils.fs.cp(f"file:{tmp_dd}", DD_PATH)
#    print("DataDictionary Excel exported to:", DD_PATH)
#except Exception as e:
#    print("Could not copy to silver volume — file at", tmp_dd, "on driver. Error:", e)

display(dd_spark.orderBy("ordinal").limit(30))