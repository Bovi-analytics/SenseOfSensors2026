# Databricks notebook source
# MAGIC %md
# MAGIC # SOS — Build the SOS24h and SOS2h datasets (one folder per farm)
# MAGIC
# MAGIC ```
# MAGIC gold_volume/
# MAGIC ├── SOS24h/  ReadMe.txt · DataDictionarySOS24h.csv
# MAGIC │   └── Farm_X/  CowDay.csv · Lactation.csv · FarmClimateDay.csv
# MAGIC └── SOS2h/   ReadMe.txt · DataDictionarySOS2h.csv
# MAGIC     └── Farm_X/  Cow2h.csv · Lactation.csv · FarmClimate2h.csv
# MAGIC ```
# MAGIC * **SOS2h** — one row per cow per day per 2-hour bin (12 per day), built from the silver sensor series.
# MAGIC   Duplicate readings for the same 15-minute window (two sensors on one cow) are averaged, so a bin
# MAGIC   never holds more than 8 fifteen-minute readings.
# MAGIC * **SOS24h** — one row per cow per day. Context, lactation and bout columns come from `GoldMaster`;
# MAGIC   the six daily behaviour totals from 15-min data (`Daily…Min15`) are the sums of the SOS2h bins, so the
# MAGIC   two datasets always agree (this also corrects gold's `DailyWalkingMin15`).
# MAGIC * **One row per cow per day.** When the same cow appears under two sensor ids on one day, the more
# MAGIC   complete row is kept and flagged (`TwoSensorIdsSameDay`).
# MAGIC * The same `CowID` is used in both datasets. Removed everywhere: life numbers, sensor ids, farm names,
# MAGIC   free-text notes.
# MAGIC
# MAGIC Reads gold + silver, writes only `SOS24h/`, `SOS2h/` and a temporary `_Staging/` (deleted at the end).

# COMMAND ----------

dbutils.widgets.text("Catalog", "senseofsensors_catalog")
dbutils.widgets.text("Farms", "All")                                    # or e.g. "Farm_A,Farm_C"
dbutils.widgets.dropdown("IdSource", "BestLifeNumberDigits", ["BestLifeNumberDigits", "AnimalIdentifier"])
dbutils.widgets.dropdown("TimestampConvention", "Auto", ["Auto", "Start", "End"])
dbutils.widgets.dropdown("OutputFormat", "CSV", ["CSV", "Parquet"])
dbutils.widgets.text("SessionTimezone", "UTC")   # must match the timezone the gold pipeline ran in

CATALOG    = dbutils.widgets.get("Catalog")
FARMS_ARG  = dbutils.widgets.get("Farms").strip()
ID_SOURCE  = dbutils.widgets.get("IdSource")
TS_CONV    = dbutils.widgets.get("TimestampConvention")
OUT_FMT    = dbutils.widgets.get("OutputFormat")
SESSION_TZ = dbutils.widgets.get("SessionTimezone")

# Day and 2-h bin boundaries depend on the Spark session timezone. Gold was built with the
# Databricks default (UTC); a cluster with a different setting would shift every bin.
spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)

TABLE_FORMAT = "delta"
GOLD    = f"/Volumes/{CATALOG}/gold/gold_volume"
SILVER  = f"/Volumes/{CATALOG}/silver/silver_volume"
OUT     = {"24h": f"{GOLD}/SOS24h", "2h": f"{GOLD}/SOS2h"}
STAGING = f"{GOLD}/_Staging"
EXT     = ".csv" if OUT_FMT == "CSV" else ".parquet"

import datetime, re
from pyspark.sql import functions as F, Window

def load(p):
    return spark.read.format(TABLE_FORMAT).load(p)

gm      = load(f"{GOLD}/GoldMaster")
dbs     = load(f"{GOLD}/DailyBehaviourSummary")
thi_day = load(f"{GOLD}/DailyTHISummary")
REF_TABLES = {n: load(f"{GOLD}/{n}") for n in ["LactationWindows", "ReproductiveTimeline", "ClinicalFlags"]}

ALL_FARMS = sorted(r[0] for r in gm.select("HerdIdentifier").distinct().collect())
FARMS = ALL_FARMS if FARMS_ARG.lower() == "all" else [f.strip() for f in FARMS_ARG.split(",") if f.strip()]
missing = sorted(set(FARMS) - set(ALL_FARMS))
if missing:
    raise ValueError(f"Unknown farms {missing}. Available: {ALL_FARMS}")
print(f"Farms: {FARMS}   id_source={ID_SOURCE}   format={OUT_FMT}")

BUILD_LOG = []          # lines that go into ReadMe.txt
def log(s=""):
    BUILD_LOG.append(s)
    print(s)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Which column identifies an individual cow?
# MAGIC `AnimalIdentifier` comes from the sensor system. When a farm reuses a cow number, one
# MAGIC `AnimalIdentifier` can belong to two different cows over time (`CowNumberReused`).
# MAGIC `BestLifeNumberDigits` is the (hashed) life number the gold pipeline resolved for each day,
# MAGIC so it follows the actual animal.

# COMMAND ----------

ids = gm.select("HerdIdentifier", "AnimalIdentifier", "BestLifeNumberDigits")
n_days      = gm.count()
n_null_best = gm.where(F.col("BestLifeNumberDigits").isNull()).count()
pairs       = ids.distinct()
aid_multi   = (pairs.where(F.col("BestLifeNumberDigits").isNotNull())
                    .groupBy("HerdIdentifier", "AnimalIdentifier")
                    .agg(F.countDistinct("BestLifeNumberDigits").alias("n")).where("n > 1").count())
life_multi  = (pairs.where(F.col("BestLifeNumberDigits").isNotNull())
                    .groupBy("HerdIdentifier", "BestLifeNumberDigits")
                    .agg(F.countDistinct("AnimalIdentifier").alias("n")).where("n > 1").count())

log(f"Cow-days in GoldMaster: {n_days:,}; without a resolved life number: {n_null_best:,} ({n_null_best/n_days:.2%})")
log(f"Cow numbers reused by another cow: {aid_multi:,}; cows seen under more than one sensor id: {life_multi:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — CowID, and one row per cow per day
# MAGIC `CowID` is the pseudonymised ID that silver and gold already carry (see the private anonymisation step
# MAGIC in the silver notebooks); no real life numbers or sensor IDs reach the export.

# COMMAND ----------

if ID_SOURCE == "BestLifeNumberDigits":
    ID_PLAIN = F.coalesce(F.col("BestLifeNumberDigits"), F.col("AnimalIdentifier"))
else:
    ID_PLAIN = F.col("AnimalIdentifier")

COW_ID = ID_PLAIN.cast("string")
ID_METHOD = f"CowID = pseudonymised {ID_SOURCE} from gold"
if ID_SOURCE == "BestLifeNumberDigits":
    ID_METHOD += "; days without a resolved life number fall back to the sensor id"

# One row per (farm, CowID, day): keep the row with the most sensor readings, flag the day.
completeness = (F.coalesce(F.col("Lying15minWindows"), F.lit(0)) +
                F.coalesce(F.col("Eating15minWindows"), F.lit(0)))
per_day = Window.partitionBy("HerdIdentifier", "CowID", "EventDay")
gm_all = (gm.where(F.col("HerdIdentifier").isin(FARMS))
            .withColumn("CowID", COW_ID)
            .withColumn("DayRelCalving", F.datediff("EventDay", "LactationStart"))
            .withColumn("_n", F.count(F.lit(1)).over(per_day))
            .withColumn("_rank", F.row_number().over(per_day.orderBy(completeness.desc(), F.col("AnimalIdentifier")))))
gm_id = (gm_all.where("_rank = 1")
               .withColumn("TwoSensorIdsSameDay", F.col("_n") > 1)
               .drop("_rank", "_n")
               .cache())

dropped = gm_all.where("_rank > 1").groupBy("HerdIdentifier").count().orderBy("HerdIdentifier").collect()
log("Same cow under two sensor ids on one day: kept the more complete row, dropped "
    + (", ".join(f"{r[0]} {r[1]:,}" for r in dropped) if dropped else "none"))

# COMMAND ----------
# MAGIC %md ## 3 — Column groups (what goes where, what is removed)

# COMMAND ----------

DROP_ID = {"AnimalIdentifier", "BestLifeNumberDigits", "RefAnimalIdentifier", "LifeNumber", "LifeNumberDigits",
           "SensorAnimalIdentifier", "SensorLifeNumber", "SensorLifeNumberDigits", "SensorLifeNumbers",
           "RefLifeNumber", "RefLifeNumberDigits", "Responders", "HerdName", "HerdKey", "AnimalKey"}
DROP_TEXT = {"Notes", "IndividualAttention1", "IndividualAttention2", "IndividualAttention3",
             "IndividualAttention4", "ClinicalObservationsOverflow"}
DROP_PIPE = {"Warnings", "WarningCount", "SilverProcessedAt"}
NEVER_EXPORT = DROP_ID | DROP_TEXT | DROP_PIPE

def present(cols, df):
    return [c for c in cols if c in df.columns]

LACT_CTX = ["Parity", "LactationStart", "LactationEnd", "LactationLengthDays", "LactationEndSource", "CalvingSeason"]
DAY_CTX  = ["DaysInMilk", "LactationPhase", "PreCalvingWeek", "DayRelCalving"]
QC       = ["HasRefData", "JoinMethod", "CowNumberReused", "IdentityConflict", "TwoSensorIdsSameDay",
            "DuplicateReadings15min"]
BEHAV    = [c for c in dbs.columns if c not in ("HerdIdentifier", "AnimalIdentifier", "EventDay")]
THI_COLS = [c for c in thi_day.columns if c not in ("HerdIdentifier", "MeasurementDate")]

# Daily totals that SOS24h takes from the SOS2h bins instead of from gold.
RECOMPUTED = {"DailyLyingMin15": "LyingMin", "DailyStandingMin15": "StandingMin", "DailyWalkingMin15": "WalkingMin",
              "DailyEatingMin15": "EatingMin", "DailyRuminatingMin15": "RuminatingMin",
              "DailyInactiveMin15": "InactiveMin", "Lying15minWindows": "PostureReadings15min",
              "Eating15minWindows": "ActivityReadings15min", "DuplicateReadings15min": "DuplicateReadings15min"}

COW_DAY_COLS = ["CowID", "HerdIdentifier", "EventDay"] + present(LACT_CTX + DAY_CTX + BEHAV, gm_id) + \
               [c for c in QC if c in gm_id.columns or c == "DuplicateReadings15min"]
LACT_LEVEL   = [c for c in gm.columns
                if c not in set(COW_DAY_COLS) | set(THI_COLS) | NEVER_EXPORT | {"EventDay"}
                and not re.search(r"THI|HeatStress|HeatLoad", c)]          # climate is day-level → climate file

print(f"CowDay columns    ({len(COW_DAY_COLS)}): {COW_DAY_COLS}")
print(f"Lactation columns from GoldMaster ({len(LACT_LEVEL)}): {LACT_LEVEL}")
print(f"Climate columns   ({len(THI_COLS)}): {THI_COLS}")
print(f"Removed everywhere: {sorted(c for c in NEVER_EXPORT if c in gm.columns)}")

# COMMAND ----------
# MAGIC %md ## 4 — Writers, data dictionary and safety checks

# COMMAND ----------

def flat(df):
    """CSV can't hold arrays/maps/structs; drop them (none are expected)."""
    bad = [f.name for f in df.schema.fields if f.dataType.typeName() in ("array", "map", "struct")]
    if bad:
        print(f"  dropping nested columns {bad}")
    return df.drop(*bad)

def assert_clean(df, label):
    leaked = sorted(set(df.columns) & NEVER_EXPORT)
    if leaked:
        raise AssertionError(f"{label}: identifying/free-text columns would be exported: {leaked}")

def exists(p):
    try:
        dbutils.fs.ls(p)
        return True
    except Exception:
        return False

def write_single(df, path, order):
    """Write df as ONE file at `path` (Spark normally writes a folder of part files)."""
    df = flat(df)
    assert_clean(df, path)
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

def camel_words(s):
    w = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", s).lower()
    return re.sub(r"\bthi\b", "THI", re.sub(r"\bbhba\b", "BHBA", w))

def cap(s):
    return s[:1].upper() + s[1:]

DESC = {
    "CowID": ("", "Individual cow identifier (hashed); identical in SOS24h and SOS2h"),
    "HerdIdentifier": ("", "Pseudonymised farm code"),
    "EventDay": ("date", "Calendar day (yyyy-MM-dd)"),
    "BinStartHour": ("h", "Start hour of the 2-hour bin (0, 2, ..., 22)"),
    "Parity": ("count", "Lactation number of the cow (1 = first calving)"),
    "LactationStart": ("date", "Calving date that started this lactation"),
    "LactationEnd": ("date", "End of this lactation (next calving, dry-off or end of data)"),
    "LactationLengthDays": ("d", "Length of the lactation"),
    "LactationEndSource": ("", "How LactationEnd was determined"),
    "CalvingSeason": ("", "Season of calving"),
    "DaysInMilk": ("d", "Days since calving (DIM)"),
    "DayRelCalving": ("d", "EventDay minus LactationStart; negative = before calving"),
    "LactationPhase": ("", "Lactation phase label for the day"),
    "PreCalvingWeek": ("", "Week before calving, for pre-calving days"),
    "HasRefData": ("boolean", "QC: the cow-day could be linked to farm reference data"),
    "JoinMethod": ("", "QC: how sensor and reference identities were linked"),
    "CowNumberReused": ("boolean", "QC: the farm cow number was reused by another animal at some point"),
    "IdentityConflict": ("boolean", "QC: identity evidence for this day was conflicting"),
    "TwoSensorIdsSameDay": ("boolean", "QC: this cow appeared under two sensor ids on this day; the more complete one is kept"),
    "DuplicateReadings15min": ("count", "QC: extra readings for an already-filled 15-min window (two sensors at once); averaged"),
    "DailyLegActivity": ("sensor units/d", "Leg activity, daily total"),
    "Eating15minWindows": ("count", "15-min windows with an eating reading that day (96 = complete)"),
    "Lying15minWindows": ("count", "15-min windows with a lying reading that day (96 = complete)"),
    "Eating2hrWindows": ("count", "2-h eating readings that day (12 = complete)"),
    "LyingBouts": ("count", "Number of lying bouts"),
    "NrLyingBouts": ("count", "Number of lying bouts (device summary)"),
    "StandupCount": ("count", "Number of stand-ups"),
    "PostureReadings15min": ("count", "15-min windows with a lying reading in this bin (8 = complete)"),
    "ActivityReadings15min": ("count", "15-min windows with an eating reading in this bin (8 = complete)"),
    "HourlyReadingsCount": ("count", "Hourly climate readings in this period"),
}

def describe_col(c):
    if c in DESC:
        return DESC[c]
    m = re.match(r"^Daily(Eating|Ruminating|Inactive|Lying|Standing|Walking)Min(15|2hr|Day)$", c)
    if m:
        src = {"15": "sum of the 15-min sensor windows (= sum of the SOS2h bins)",
               "2hr": "summed from the sensor's 2-h series", "Day": "device daily total"}[m.group(2)]
        return ("min/d", f"{m.group(1)} time per day, {src}")
    m = re.match(r"^(Eating|Ruminating|Inactive|Lying|Standing|Walking)Min$", c)
    if m:
        return ("min/2h", f"{m.group(1)} time in the 2-h bin, summed from 15-min sensor windows (0-120)")
    m = re.match(r"^(Eating|Ruminating|Inactive)Min2hrSensor$", c)
    if m:
        return ("min/2h", f"{m.group(1)} time in the 2-h bin as reported by the sensor's own 2-h series")
    m = re.match(r"^Note(.+)$", c)
    if m:
        return ("boolean", f"Farm notes for this lactation mention: {camel_words(m.group(1))}")
    if re.search(r"BoutLength|BoutLowerQuartile|BoutUpperQuartile", c):
        return ("min", cap(camel_words(c)))
    if re.search(r"THI", c):
        return ("index" if "Hours" not in c else "h", cap(camel_words(c)))
    if c.startswith("Dim"):
        return ("d", f"Days in milk at: {camel_words(c[3:])}")
    if re.search(r"Date$", c):
        return ("date", cap(camel_words(c)))
    if c.startswith("Bcs"):
        return ("score 1-5", cap(camel_words(c).replace("bcs", "body condition score")))
    if c.startswith("Loco"):
        return ("score 1-5", cap(camel_words(c).replace("loco", "locomotion score")))
    if c in ("CalciumLevel",):
        return ("mmol/L", "Blood calcium")
    if c.startswith("BHBA"):
        return ("mmol/L", f"Blood beta-hydroxybutyrate, sample {c[-1]}")
    if "Celsius" in c:
        return ("degC", cap(camel_words(c)))
    if "Pct" in c:
        return ("%", cap(camel_words(c)))
    return ("", "")

def dictionary_rows(file_label, df):
    num = [f.name for f in df.schema.fields if f.dataType.typeName() in
           ("integer", "long", "double", "float", "short", "decimal", "date")]
    aggs = [F.count(F.lit(1)).alias("__n")] + [F.count(F.col(c)).alias(f"nn__{c}") for c in df.columns]
    aggs += [F.min(c).alias(f"mn__{c}") for c in num] + [F.max(c).alias(f"mx__{c}") for c in num]
    r = df.agg(*aggs).first().asDict()
    rows = []
    for f in df.schema.fields:
        unit, desc = describe_col(f.name)
        rows.append((file_label, f.name, f.dataType.simpleString(), unit, desc,
                     round(100 * r[f"nn__{f.name}"] / max(r["__n"], 1), 1),
                     str(r.get(f"mn__{f.name}", "")) if f.name in num else "",
                     str(r.get(f"mx__{f.name}", "")) if f.name in num else ""))
    return rows

def write_dictionary(rows, path):
    cols = ["File", "Column", "Type", "Unit", "Description", "PctNonNull", "Min", "Max"]
    write_single(spark.createDataFrame(rows, cols), path, ["File"])

def write_readme(root, title, files_note, extra):
    built = datetime.datetime.now(datetime.timezone.utc)
    txt = "\n".join([title, "=" * len(title), f"Built: {built:%Y-%m-%d %H:%M} UTC",
                     f"Farms: {', '.join(FARMS)}", "", files_note, "", "Identifiers", "-----------", ID_METHOD,
                     "Removed: life numbers, sensor/animal identifiers, farm names, free-text notes.", ""]
                    + extra + [""])
    dbutils.fs.put(f"{root}/ReadMe.txt", txt, overwrite=True)

# COMMAND ----------
# MAGIC %md ## 5 — Lactation table (shared by SOS24h and SOS2h)

# COMMAND ----------

KEYS = ["HerdIdentifier", "CowID", "LactationStart"]
bool_cols = {f.name for f in gm_id.schema.fields if f.dataType.typeName() == "boolean"}

lact = (gm_id.where(F.col("LactationStart").isNotNull())
             .groupBy(*KEYS)
             .agg(*[(F.max(c) if c in bool_cols else F.first(c, ignorenulls=True)).alias(c)
                    for c in present(["Parity", "LactationEnd", "LactationLengthDays", "LactationEndSource",
                                      "CalvingSeason"], gm_id) + LACT_LEVEL],
                  F.first("AnimalIdentifier", ignorenulls=True).alias("_aid"),
                  F.first("RefAnimalIdentifier", ignorenulls=True).alias("_rid")))

# Columns only present in the reference tables (e.g. DimCalcium, DimBhba1/2).
# Their AnimalIdentifier may be the sensor-side or the reference-side id; try both, keep the better match.
for name, ref in REF_TABLES.items():
    extra = [c for c in ref.columns if c not in gm.columns and c not in lact.columns and c not in NEVER_EXPORT
             and c not in ("HerdIdentifier", "AnimalIdentifier", "LactationStart")]
    if not extra:
        continue
    if "LactationStart" not in ref.columns:
        log(f"  {name}: no LactationStart column, skipped extra columns {extra}")
        continue
    r = ref.select("HerdIdentifier", F.col("AnimalIdentifier").alias("_k"), "LactationStart", *extra) \
           .dropDuplicates(["HerdIdentifier", "_k", "LactationStart"])
    hits = {k: lact.join(r.withColumnRenamed("_k", k), ["HerdIdentifier", k, "LactationStart"]).count()
            for k in ["_rid", "_aid"]}
    best = max(hits, key=hits.get)
    if hits[best] == 0:
        log(f"  {name}: could not link to GoldMaster lactations; skipped {extra}")
        continue
    lact = lact.join(r.withColumnRenamed("_k", best), ["HerdIdentifier", best, "LactationStart"], "left")
    log(f"  {name}: added {extra} (linked {hits[best]:,} lactations via "
        f"{'reference' if best == '_rid' else 'sensor'} AnimalIdentifier)")

lact = lact.drop("_aid", "_rid").cache()
dup_lact = lact.groupBy(*KEYS).count().where("count > 1").count()
log(f"Lactations: {lact.count():,}; duplicates on (farm, CowID, LactationStart): {dup_lact}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 — Calibrate units and timestamp convention of the sensor series
# MAGIC A reading's timestamp can mark the **start** of its window (00:00 = 00:00–00:15) or the **end**
# MAGIC (00:00 = 23:45–00:00). Both are tried on a sample of cows and compared with gold's daily totals and
# MAGIC with the device's own daily totals. The same comparison detects minutes vs seconds.

# COMMAND ----------

B15 = {"LyingTimeData": "LyingMin", "StandingTimeData": "StandingMin", "WalkingTimeData": "WalkingMin",
       "EatingTimeData15min": "EatingMin", "RuminatingTimeData15min": "RuminatingMin",
       "InactiveTimeData15min": "InactiveMin"}
B2H = {"EatingTimeData2hr": "EatingMin2hrSensor", "RuminatingTimeData2hr": "RuminatingMin2hrSensor",
       "InactiveTimeData2hr": "InactiveMin2hrSensor"}
GOLD_DAILY = {"LyingTimeData": "DailyLyingMin15", "StandingTimeData": "DailyStandingMin15",
              "WalkingTimeData": "DailyWalkingMin15", "EatingTimeData15min": "DailyEatingMin15",
              "RuminatingTimeData15min": "DailyRuminatingMin15", "InactiveTimeData15min": "DailyInactiveMin15",
              "EatingTimeData2hr": "DailyEatingMin2hr", "RuminatingTimeData2hr": "DailyRuminatingMin2hr",
              "InactiveTimeData2hr": "DailyInactiveMin2hr"}
DEVICE_DAILY = {"EatingTimeData15min": "DailyEatingMinDay", "RuminatingTimeData15min": "DailyRuminatingMinDay",
                "InactiveTimeData15min": "DailyInactiveMinDay"}

SCALE, CONV, RELIABLE = {}, {}, {}

sensor = load(f"{SILVER}/SensorData")
try:
    avail = {f.name.rstrip("/").split("=", 1)[1] for f in dbutils.fs.ls(f"{SILVER}/SensorData")
             if f.name.startswith("ObservationType=")}
except Exception:
    avail = set()
if not avail:
    avail = {r[0] for r in sensor.select("ObservationType").distinct().collect()}
B15 = {k: v for k, v in B15.items() if k in avail}
B2H = {k: v for k, v in B2H.items() if k in avail}
TYPES = list(B15) + list(B2H)
print(f"15-min series: {list(B15)}\n2-h series:    {list(B2H)}")
if "LyingTimeData" not in B15:
    raise ValueError("LyingTimeData not found in silver SensorData; cannot build posture bins.")

farm0 = FARMS[0]
sample_ids = [r[0] for r in gm.where((F.col("HerdIdentifier") == farm0) & F.col("DailyLyingMin15").isNotNull())
                              .groupBy("AnimalIdentifier").count().orderBy(F.desc("count")).limit(15).collect()]
gsample = (gm.where((F.col("HerdIdentifier") == farm0) & F.col("AnimalIdentifier").isin(sample_ids))
             .select("AnimalIdentifier", "EventDay", *present(set(GOLD_DAILY.values()) | set(DEVICE_DAILY.values()), gm)))
ssample = (sensor.where(F.col("ObservationType").isin(TYPES) & (F.col("HerdIdentifier") == farm0)
                        & F.col("AnimalIdentifier").isin(sample_ids))
                 .select("AnimalIdentifier", "EventDate", "ObservationType", "ObservationValue").cache())
if ssample.limit(1).count() == 0:
    raise ValueError(f"No silver sensor rows match GoldMaster ids for {farm0}.")

def daily_sums(t, conv):
    eff = F.col("EventDate") - F.expr("INTERVAL 1 MINUTE") if conv == "End" else F.col("EventDate")
    return (ssample.where(F.col("ObservationType") == t)
                   .groupBy("AnimalIdentifier", F.to_date(eff).alias("EventDay"))
                   .agg(F.sum("ObservationValue").alias("s")))

def compare(t, conv, gcol, scale=None):
    j = (daily_sums(t, conv).join(gsample.select("AnimalIdentifier", "EventDay", F.col(gcol).alias("g")),
                                  ["AnimalIdentifier", "EventDay"])
                            .where(F.col("g").isNotNull()))
    if scale is None:
        ratio = j.where("g > 30").select(F.percentile_approx(F.col("s") / F.col("g"), 0.5)).first()[0]
        if ratio is None:
            return None
        # Only ever minutes (x1) or seconds (/60). Any other ratio means the reference itself is off.
        scale = 1 / 60 if 40 < ratio < 80 else 1.0
    r = j.select(F.avg(F.abs(F.col("s") * scale - F.col("g"))).alias("mae"),
                 F.avg((F.abs(F.col("s") * scale - F.col("g")) <= 1).cast("double")).alias("within1"),
                 F.count("*").alias("n")).first()
    return {"scale": scale, "mae": r["mae"], "within1": r["within1"], "n": r["n"]}

log("")
log(f"Calibration against gold daily totals (sample: 15 cows on {farm0})")
log(f"  {'series':26s} {'conv':5s} {'scale':>7s} {'days':>6s} {'mean |diff| min':>16s} {'within 1 min':>13s}")
for t in TYPES:
    gcol = GOLD_DAILY.get(t)
    res = {c: compare(t, c, gcol) for c in ["Start", "End"]} if gcol in gm.columns else {}
    res = {c: v for c, v in res.items() if v and v["mae"] is not None}
    if not res:
        SCALE[t], CONV[t] = 1.0, ("Start" if TS_CONV == "Auto" else TS_CONV)
        log(f"  {t:26s} nothing to compare; using scale 1, {CONV[t]}")
        continue
    for c, v in res.items():
        log(f"  {t:26s} {c:5s} {v['scale']:7.4f} {v['n']:6d} {v['mae']:16.2f} {v['within1']:13.1%}")
    best = min(res, key=lambda c: res[c]["mae"])
    CONV[t] = best if TS_CONV == "Auto" else TS_CONV
    SCALE[t] = res.get(CONV[t], res[best])["scale"]
    RELIABLE[t] = res[best]["within1"]
    if res[best]["within1"] < 0.9:
        log(f"  NOTE {t}: sensor sums and gold's {gcol} disagree on many days; the sensor sums are used.")

# A series whose gold total is unreliable (walking) can't tell us its timestamp convention.
# Series from the same sensor share one convention, so take it from the most reliable sibling.
if TS_CONV == "Auto":
    for group in [["LyingTimeData", "StandingTimeData", "WalkingTimeData"],
                  ["EatingTimeData15min", "RuminatingTimeData15min", "InactiveTimeData15min"],
                  list(B2H)]:
        members = [t for t in group if t in CONV]
        good = [t for t in members if RELIABLE.get(t, 0) >= 0.9]
        if not good:
            continue
        ref_t = max(good, key=lambda t: RELIABLE[t])
        for t in members:
            if RELIABLE.get(t, 0) < 0.9 and CONV[t] != CONV[ref_t]:
                CONV[t] = CONV[ref_t]
                log(f"  {t}: timestamp convention taken from {ref_t} ({CONV[t]}), same sensor")

log("Device daily totals (independent of gold) — which convention fits better:")
for t, dcol in DEVICE_DAILY.items():
    if t in B15 and dcol in gm.columns:
        res = {c: compare(t, c, dcol, SCALE[t]) for c in ["Start", "End"]}
        parts = [f"{c}: mean |diff| {v['mae']:.1f} min, within 1 min {v['within1']:.0%}"
                 for c, v in res.items() if v and v["mae"] is not None]
        log(f"  {t:26s} " + " | ".join(parts))
ssample.unpersist()
log("Chosen: " + ", ".join(f"{t}={CONV[t]}/x{SCALE[t]:.4g}" for t in TYPES))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 — Build the 2-hour bins
# MAGIC Every reading is assigned to its 15-min window (2-h window for the 2-h series). When a window has more
# MAGIC than one reading — two sensors on the same cow at the same time — the readings are **averaged**, so
# MAGIC each window counts once. Bins are then summed per cow, day and 2-h block, on a full grid of 12 bins
# MAGIC for every cow-day in SOS24h. Staged once, then split per farm.

# COMMAND ----------

end_types = [t for t in TYPES if CONV[t] == "End"]
eff = (F.when(F.col("ObservationType").isin(end_types), F.col("EventDate") - F.expr("INTERVAL 1 MINUTE"))
         .otherwise(F.col("EventDate")) if end_types else F.col("EventDate"))
scale_map = F.create_map(*[F.lit(x) for t in TYPES for x in (t, float(SCALE[t]))])

readings = (sensor.where(F.col("ObservationType").isin(TYPES) & F.col("HerdIdentifier").isin(FARMS))
                  .select("HerdIdentifier", "AnimalIdentifier", "ObservationType",
                          (F.col("ObservationValue") * scale_map[F.col("ObservationType")]).alias("v"),
                          eff.alias("eff"))
                  .withColumn("EventDay", F.to_date("eff"))
                  .withColumn("BinStartHour", (F.floor(F.hour("eff") / 2) * 2).cast("int"))
                  .withColumn("Win", F.when(F.col("ObservationType").isin(list(B2H)), F.lit(-1))
                                      .otherwise(F.floor(F.unix_timestamp("eff") / 900))))
windows = (readings.groupBy("HerdIdentifier", "AnimalIdentifier", "ObservationType", "EventDay", "BinStartHour", "Win")
                   .agg(F.avg("v").alias("v"), F.count(F.lit(1)).alias("nraw")))

aggs = [F.sum(F.when(F.col("ObservationType") == t, F.col("v"))).alias(n) for t, n in {**B15, **B2H}.items()]
aggs.append(F.count(F.when(F.col("ObservationType") == "LyingTimeData", 1)).alias("PostureReadings15min"))
if "EatingTimeData15min" in B15:
    aggs.append(F.count(F.when(F.col("ObservationType") == "EatingTimeData15min", 1)).alias("ActivityReadings15min"))
aggs.append(F.sum(F.when(F.col("ObservationType").isin(list(B15)), F.col("nraw") - 1).otherwise(0))
             .cast("long").alias("DuplicateReadings15min"))
bins = windows.groupBy("HerdIdentifier", "AnimalIdentifier", "EventDay", "BinStartHour").agg(*aggs)

day_keys = gm_id.select("HerdIdentifier", "AnimalIdentifier", "EventDay", "CowID",
                        *present(["Parity", "LactationStart", "DaysInMilk", "DayRelCalving", "LactationPhase"], gm_id))
hours = spark.range(0, 24, 2).select(F.col("id").cast("int").alias("BinStartHour"))
count_cols = present(["PostureReadings15min", "ActivityReadings15min", "DuplicateReadings15min"], bins)
cow2h = (day_keys.crossJoin(hours)
                 .join(bins, ["HerdIdentifier", "AnimalIdentifier", "EventDay", "BinStartHour"], "left")
                 .fillna(0, subset=count_cols)
                 .drop("AnimalIdentifier"))
ordered = (["CowID", "HerdIdentifier", "EventDay", "BinStartHour"] +
           present(["Parity", "LactationStart", "DaysInMilk", "DayRelCalving", "LactationPhase"], cow2h) +
           list(B15.values()) + list(B2H.values()) + count_cols)
(cow2h.select(*ordered).write.format(TABLE_FORMAT).mode("overwrite")
      .partitionBy("HerdIdentifier").save(f"{STAGING}/Cow2h"))
cow2h = load(f"{STAGING}/Cow2h")
print(f"staged {cow2h.count():,} Cow2h rows")

thi_hr = load(f"{SILVER}/TemperatureHumidityIndex")
hour = F.coalesce(F.col("MeasurementHour"), F.hour("MeasurementDatetime"))
day = F.coalesce(F.col("MeasurementDate"), F.to_date("MeasurementDatetime"))
clim2h = (thi_hr.where(F.col("HerdIdentifier").isin(FARMS))
                .withColumn("EventDay", day)
                .withColumn("BinStartHour", (F.floor(hour / 2) * 2).cast("int"))
                .groupBy("HerdIdentifier", "EventDay", "BinStartHour")
                .agg(F.avg("THIExternal").alias("MeanTHIExternal"), F.max("THIExternal").alias("MaxTHIExternal"),
                     F.avg("THIInternal").alias("MeanTHIInternal"), F.max("THIInternal").alias("MaxTHIInternal"),
                     F.avg("TempExternalCelsius").alias("MeanTempExternalCelsius"),
                     F.avg("TempInternalCelsius").alias("MeanTempInternalCelsius"),
                     F.avg("HumidityExternalPct").alias("MeanHumidityExternalPct"),
                     F.avg("HumidityInternalPct").alias("MeanHumidityInternalPct"),
                     F.count("*").alias("HourlyReadingsCount")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8 — SOS24h: daily rows, with the 15-min daily totals taken from the bins

# COMMAND ----------

daily_bins = (cow2h.groupBy("HerdIdentifier", "CowID", "EventDay")
                   .agg(*[F.sum(src).alias(dst) for dst, src in RECOMPUTED.items() if src in cow2h.columns]))
recomputed_cols = [c for c in RECOMPUTED if c in daily_bins.columns]

cow_day = (gm_id.drop(*[c for c in recomputed_cols if c in gm_id.columns])
                .join(daily_bins, ["HerdIdentifier", "CowID", "EventDay"], "left")
                .select(*[c for c in COW_DAY_COLS if c in gm_id.columns or c in recomputed_cols]))
clim24 = (thi_day.where(F.col("HerdIdentifier").isin(FARMS))
                 .withColumnRenamed("MeasurementDate", "EventDay"))

root = OUT["24h"]
sizes24 = []
for farm in FARMS:
    d = f"{root}/{farm}"
    n1 = write_single(cow_day.where(F.col("HerdIdentifier") == farm), f"{d}/CowDay{EXT}", ["CowID", "EventDay"])
    n2 = write_single(lact.where(F.col("HerdIdentifier") == farm), f"{d}/Lactation{EXT}", ["CowID", "LactationStart"])
    n3 = write_single(clim24.where(F.col("HerdIdentifier") == farm), f"{d}/FarmClimateDay{EXT}", ["EventDay"])
    sizes24.append(f"  {farm}: CowDay {n1/1e6:,.1f} MB · Lactation {n2/1e6:,.1f} MB · FarmClimateDay {n3/1e6:,.1f} MB")
    print(sizes24[-1])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9 — Validation, SOS2h files, ReadMe files, clean up
# MAGIC Independent checks: on days with all 96 readings, lying + standing + walking and eating + ruminating +
# MAGIC inactive should each add up to 1,440 minutes. Also reported: how many days differ from gold's totals.

# COMMAND ----------

val = ["", "Validation", "----------"]
cd = cow_day.cache()
post = F.col("DailyLyingMin15") + F.col("DailyStandingMin15") + F.col("DailyWalkingMin15")
act = F.col("DailyEatingMin15") + F.col("DailyRuminatingMin15") + F.col("DailyInactiveMin15")
r = cd.select(
    F.sum((F.col("Lying15minWindows") == 96).cast("int")).alias("np"),
    F.avg(F.when(F.col("Lying15minWindows") == 96, (F.abs(post - 1440) <= 1).cast("double"))).alias("pp"),
    F.sum((F.col("Eating15minWindows") == 96).cast("int")).alias("na"),
    F.avg(F.when(F.col("Eating15minWindows") == 96, (F.abs(act - 1440) <= 1).cast("double"))).alias("pa")).first()
val.append(f"Lying + standing + walking = 1440 min on complete days: {r['pp'] or 0:.1%} of {r['np'] or 0:,}")
val.append(f"Eating + ruminating + inactive = 1440 min on complete days: {r['pa'] or 0:.1%} of {r['na'] or 0:,}")

gold_cmp = gm_id.select("HerdIdentifier", "CowID", "EventDay",
                        *[F.col(c).alias(f"gold_{c}") for c in RECOMPUTED if c.startswith("Daily") and c in gm_id.columns])
j = cd.join(gold_cmp, ["HerdIdentifier", "CowID", "EventDay"])
val.append("Days where SOS24h differs from gold's original value by more than 1 min:")
for c in [c for c in RECOMPUTED if c.startswith("Daily") and f"gold_{c}" in j.columns]:
    rr = j.where(F.col(c).isNotNull() & F.col(f"gold_{c}").isNotNull()) \
          .select(F.count("*").alias("n"),
                  F.avg((F.abs(F.col(c) - F.col(f"gold_{c}")) > 1).cast("double")).alias("p")).first()
    if rr["n"]:
        val.append(f"  {c:22s} {rr['p']:6.1%} of {rr['n']:,} days")

dup24 = cd.groupBy("HerdIdentifier", "CowID", "EventDay").count().where("count > 1").count()
dup2 = cow2h.groupBy("HerdIdentifier", "CowID", "EventDay", "BinStartHour").count().where("count > 1").count()
over = cow2h.where(F.greatest(*[F.col(c) for c in ["PostureReadings15min", "ActivityReadings15min"]
                                if c in cow2h.columns]) > 8).count()
dupr = cow2h.where(F.col("DuplicateReadings15min") > 0).count() if "DuplicateReadings15min" in cow2h.columns else 0
two = cd.where(F.col("TwoSensorIdsSameDay")).count() if "TwoSensorIdsSameDay" in cd.columns else 0
val += [f"Duplicate (farm, CowID, EventDay) rows in SOS24h: {dup24}",
        f"Duplicate (farm, CowID, EventDay, BinStartHour) rows in SOS2h: {dup2}",
        f"2-h bins with more than 8 fifteen-minute readings: {over}",
        f"2-h bins where duplicate readings were averaged: {dupr:,}",
        f"Cow-days that had two sensor ids (more complete one kept): {two:,}"]
for line in val:
    log(line)

root = OUT["2h"]
sizes2 = []
for farm in FARMS:
    d = f"{root}/{farm}"
    n1 = write_single(cow2h.where(F.col("HerdIdentifier") == farm), f"{d}/Cow2h{EXT}",
                      ["CowID", "EventDay", "BinStartHour"])
    n2 = write_single(lact.where(F.col("HerdIdentifier") == farm), f"{d}/Lactation{EXT}", ["CowID", "LactationStart"])
    n3 = write_single(clim2h.where(F.col("HerdIdentifier") == farm), f"{d}/FarmClimate2h{EXT}",
                      ["EventDay", "BinStartHour"])
    sizes2.append(f"  {farm}: Cow2h {n1/1e6:,.1f} MB · Lactation {n2/1e6:,.1f} MB · FarmClimate2h {n3/1e6:,.1f} MB")
    print(sizes2[-1])

notes = ["Notes", "-----",
         f"Days and 2-h bins are computed in the {SESSION_TZ} timezone from the sensor timestamps.",
         "Readings for the same 15-min window (two sensors on one cow) are averaged; each window counts once.",
         "The six Daily...Min15 totals in SOS24h are the sums of the SOS2h bins. Gold's DailyWalkingMin15 "
         "under-counts walking (lying + standing + walking did not add up to 1,440 min); these totals replace it.",
         "Open question: the device daily totals partly suggest timestamps mark the END of each window; this "
         "build treats them as the START (matching gold). See the device comparison in the build log.",
         "An empty value means no reading."]

write_dictionary(dictionary_rows("CowDay", cd) + dictionary_rows("Lactation", lact) +
                 dictionary_rows("FarmClimateDay", clim24), f"{OUT['24h']}/DataDictionarySOS24h{EXT}")
write_readme(OUT["24h"], "SOS24h — SenseOfSensors daily dataset",
             "Per farm: CowDay (one row per cow per day), Lactation (one row per cow per lactation), "
             "FarmClimateDay (one row per farm per day). Link with CowID / LactationStart / EventDay.",
             ["Sizes", "-----"] + sizes24 + [""] + notes + ["", "Build log", "---------"] + BUILD_LOG)

write_dictionary(dictionary_rows("Cow2h", cow2h) + dictionary_rows("Lactation", lact) +
                 dictionary_rows("FarmClimate2h", clim2h), f"{OUT['2h']}/DataDictionarySOS2h{EXT}")
write_readme(OUT["2h"], "SOS2h — SenseOfSensors 2-hour dataset",
             "Per farm: Cow2h (one row per cow per day per 2-h bin; 12 bins per cow-day, same cow-days as "
             "SOS24h), Lactation (identical to SOS24h), FarmClimate2h (one row per farm per 2-h bin).",
             ["Sizes", "-----"] + sizes2 + [""] + notes + ["", "Build log", "---------"] + BUILD_LOG)

cd.unpersist()
lact.unpersist()
gm_id.unpersist()
dbutils.fs.rm(STAGING, recurse=True)
print("Done. SOS24h and SOS2h written; staging removed.")
