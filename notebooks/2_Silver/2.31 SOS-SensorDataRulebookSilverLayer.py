# Databricks notebook source
BRONZE_SENSOR = "/Volumes/senseofsensors_catalog/bronze/bronze_volume/SensorData"

for f in sorted(dbutils.fs.ls(BRONZE_SENSOR)):
    try:
        df = spark.read.parquet(f.path).limit(1)
        print(f"\n{'='*55}")
        print(f"FOLDER: {f.name}")
        for col, dtype in df.dtypes:
            val = df.select(col).first()[0]
            print(f"  {col:35} {dtype:12} {str(val)[:40]}")
    except Exception as e:
        print(f"\nFOLDER: {f.name}  ERROR: {e}")

# COMMAND ----------

BRONZE_SENSOR = "/Volumes/senseofsensors_catalog/bronze/bronze_volume/SensorData"

for f in sorted(dbutils.fs.ls(BRONZE_SENSOR)):
    df = spark.read.parquet(f.path)
    print(f"{df.count():>12,}  {f.name}")

# COMMAND ----------

# Databricks notebook source
# SOS SensorData — Rulebook
# Declares rules for SensorData (all 23 sub-folders) and appends into the shared metadata tables.
# All 23 folders share the same 6-column schema. They union into one silver table
# silver.SensorData, partitioned by ObservationType.
# Run after: SOS_RefData_Rulebook and SOS_THI_Rulebook (so shared tables exist).
# Run before: SOS_SensorData_Silver_Cleaning.

dbutils.widgets.text("catalog", "senseofsensors_catalog")
dbutils.widgets.text("export_xlsx_path",
    "/Volumes/senseofsensors_catalog/silver/silver_volume/SOS-SensorData-Rulebook.xlsx")

CATALOG          = dbutils.widgets.get("catalog")
EXPORT_XLSX_PATH = dbutils.widgets.get("export_xlsx_path")
SILVER_SCHEMA    = f"{CATALOG}.silver"
DATASET          = "SensorData"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
print("Silver schema:", SILVER_SCHEMA, "| Dataset:", DATASET)


# Column rulebook
# All 23 sub-folders share the same 6-column schema.
# ObservationValue: always numeric but stored as string in bronze — cast to double in silver.
# AttributeMap: map keys normalised to PascalCase in silver for cross-dataset consistency.
# EventDate: already a proper timestamp in bronze parquet — validated only, not re-parsed.

# Tuple layout (16 values — no KeepRaw field, SensorData has no raw backup columns):
#   0  SourceColumn
#   1  TargetColumn
#   2  DataType
#   3  IsRequired
#   4  MinValue
#   5  MaxValue
#   6  CanonicalUnit
#   7  AllowedUnits
#   8  SemanticGroup
#   9  Description
#  10  CleaningStrategy
#  11  AcceptedFormats
#  12  InvalidAction
#  13  WarningType
#  14  Pattern
#  15  AllowedValues

sensor_rules = [

    # HerdIdentifier: same 8 farms as ReferenceData and THI.
    ('HerdIdentifier', 'HerdIdentifier', 'string', True, None, None, None, None,
     'Identity',
     'Farm / herd identifier. Same 8 farms as ReferenceData and THI. '
     'Matches ReferenceData.HerdIdentifier and THI.HerdIdentifier for cross-dataset joins.',
     'Trim;NormalizeToString',
     None, 'Warn', 'InvalidIdentifier', None, None),

    # AnimalIdentifier: 1,385 distinct animals across all farms.
    # Matches ReferenceData.AnimalIdentifier for joins.
    ('AnimalIdentifier', 'AnimalIdentifier', 'string', True, None, None, None, None,
     'Identity',
     'Animal number within the herd. 1,385 distinct animals observed. '
     'Matches ReferenceData.AnimalIdentifier for cross-dataset joins.',
     'Trim;NormalizeToString',
     None, 'Warn', 'InvalidIdentifier', None, None),

    # EventDate: already typed as TimestampType in bronze parquet.
    # Data range: 2016-01-01 to 2020-12-30 per profile report.
    # Validated only — no re-parsing needed.
    # Future dates and dates before 2016-01-01 flagged with warnings (value kept).
    ('EventDate', 'EventDate', 'timestamp', True, None, None, None, None,
     'Time',
     'Measurement timestamp. Already typed as timestamp in bronze parquet — validated only. '
     'Data range: 2016-01-01 to 2020-12-30. '
     'Future dates and dates before 2016-01-01 -> FutureDate or OldDate warning (value kept).',
     'ValidateTimestamp',
     None, 'Warn', 'FutureDate|OldDate', None, None),

    # ObservationType: 35 distinct sensor measurement types.
    # Values are PascalCase folder names (e.g. EatingTimeData15Min, LyingBoutSummary).
    # Raw values from some folders may have mixed case — normalised to PascalCase in silver.
    ('ObservationType', 'ObservationType', 'string', True, None, None, None, None,
     'Measurement',
     'Sensor measurement type. 35 distinct types observed (e.g. EatingTimeData15Min, '
     'LyingBoutSummary, LegActivityData). '
     'Normalised to PascalCase in silver to match partition folder names. '
     'Silver table is partitioned by ObservationType.',
     'Trim;NormalizeToPascalCase',
     None, 'Warn', 'UnknownObservationType', None, None),

    # ObservationValue: numeric measurement, stored as string in bronze.
    # Zero is a valid measurement (e.g. zero leg activity, zero eating time).
    # 5.28% null — expected by design for some ObservationTypes.
    # No min/max range declared — valid range varies widely by ObservationType
    # (e.g. 0-15 for 15-min time windows, 0-495994 for legActivityData outliers).
    # Per-type range validation deferred to Silver Layer 2 / gold.
    ('ObservationValue', 'ObservationValue', 'double', False, None, None, None, None,
     'Measurement',
     'Numeric measurement value. Stored as string in bronze; cast to double in silver. '
     'Zero is a valid value. 5.28% null by design for some ObservationTypes. '
     'No global range declared — valid range varies by ObservationType. '
     'Per-type validation deferred to Silver Layer 2 or gold.',
     'DoubleCast',
     None, 'Warn', 'InvalidNumeric', None, None),

    # AttributeMap: variable key-value metadata, varies by ObservationType.
    # Keys observed in profile (10 distinct keys across all rows):
    #   Responder        (51.7% of rows) — sensor tag ID
    #   InstallationId   (51.7% of rows) — sensor installation reference (was: Installationid)
    #   LifeNumber       (51.7% of rows) — national life number (was: Lifenumber + lifeNumber)
    #   NumberOfQuarters (11.8% of rows) — milk quarters involved (was: numberOfQuarters)
    #   IntervalSincePrevious (0.8% of rows) — time since previous event (was: intervalSincePrevious)
    #   StopDateTime     (0.8% of rows) — end of lying bout (was: stopDateTime)
    #   Minutes          (0.8% of rows) — bout duration in minutes (was: minutes)
    #   Attention        (0.6% of rows) — attention flag (was: attention)
    #   RollingAverage   (0.6% of rows) — rolling average value (was: rollingAverage)
    # Note: lifeNumber and Lifenumber are the same field — both normalised to LifeNumber.
    # Silver Layer 2 will crack open AttributeMap into typed columns (deferred).
    ('AttributeMap', 'AttributeMap', 'map<string,string>', False, None, None, None, None,
     'Metadata',
     'Variable key-value metadata per observation. Keys normalised to PascalCase in silver: '
     'Installationid -> InstallationId, '
     'Lifenumber / lifeNumber -> LifeNumber (merged), '
     'numberOfQuarters -> NumberOfQuarters, '
     'intervalSincePrevious -> IntervalSincePrevious, '
     'stopDateTime -> StopDateTime, '
     'minutes -> Minutes, '
     'attention -> Attention, '
     'rollingAverage -> RollingAverage. '
     'Silver Layer 2 will expand this into typed columns (deferred).',
     'NormaliseMapKeysPascalCase',
     None, None, None, None, None),
]
print(f"{DATASET}: {len(sensor_rules)} column rules")


# Cleaning constraints
sensor_constraints = [

    # Column-level fixes
    ('CF01', 'Column', '*',
     'TrimWhitespace', '',
     'Fix', '', 'Trim leading/trailing whitespace on all string columns'),

    ('CF02', 'Column', '*',
     'BlankToNull', 'Tokens=|Null|NULL|null|NA|N/A|NaN|nan|-|.|?| ',
     'Fix', '', 'Map empty-ish strings, lone space, and ? to NULL'),

    ('CF03', 'Column', 'HerdIdentifier|AnimalIdentifier',
     'RejectValues', 'Values=0|00|000|NA|N/A|None|NONE|NULL|null',
     'Fix', 'Warning', 'Placeholder identifiers -> NULL + InvalidIdentifier warning'),

    # Row-level checks
    # EventDate range: data should fall within 2016-2020 per profile report.
    # Dates before 2016-01-01 or after today flagged with warning — value kept.
    ('RC01', 'Row', 'EventDate',
     'TimestampRangeCheck', 'Min=2016-01-01;Max=today',
     'Flag', 'Warning',
     'EventDate before 2016-01-01 -> OldDate warning (value kept). '
     'EventDate after today -> FutureDate warning (value kept).'),

    # Deduplication: exact match on all 5 typed columns.
    # AttributeMap excluded from dedup key — map comparison is expensive at 1.2B rows
    # and two rows with the same 5 typed keys but different AttributeMap content
    # represent the same observation recorded twice (AttributeMap differences are noise).
    # 1,985 duplicates were removed in the previous run.
    ('DK01', 'Row', 'HerdIdentifier|AnimalIdentifier|EventDate|ObservationType|ObservationValue',
     'DedupExact',
     'Keys=HerdIdentifier|AnimalIdentifier|EventDate|ObservationType|ObservationValue',
     'Fix', '',
     'Remove exact duplicates on the 5 typed columns. AttributeMap excluded from key '
     '(map comparison too expensive at 1.2B rows; differing AttributeMap on same observation = noise). '
     '1,985 duplicates removed in previous run. Keep most-complete row.'),
]
print(f"{DATASET}: {len(sensor_constraints)} constraints")


# Persist into shared tables
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, BooleanType)

def replace_dataset(df, table):
    fq = f"{SILVER_SCHEMA}.{table}"
    if spark.catalog.tableExists(fq):
        spark.sql(f"DELETE FROM {fq} WHERE Dataset = '{DATASET}'")
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(fq)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)
    print(f"  {table}: {df.count()} rows for {DATASET}")

RULE_COLS = [
    "SourceColumn", "TargetColumn", "DataType", "IsRequired",
    "MinValue", "MaxValue", "CanonicalUnit", "AllowedUnits",
    "SemanticGroup", "Description", "CleaningStrategy",
    "AcceptedFormats", "InvalidAction", "WarningType", "Pattern", "AllowedValues",
]
rule_schema = StructType(
    [StructField("Dataset", StringType()), StructField("Ordinal", IntegerType())]
    + [StructField(c, StringType() if c not in ("IsRequired", "MinValue", "MaxValue")
                   else (BooleanType() if c == "IsRequired" else DoubleType()))
       for c in RULE_COLS]
)
rule_data = [
    (DATASET, i,
     r[0], r[1], r[2], r[3],
     None if r[4] is None else float(r[4]),
     None if r[5] is None else float(r[5]),
     r[6], r[7], r[8], r[9],
     r[10], r[11], r[12], r[13], r[14], r[15])
    for i, r in enumerate(sensor_rules)
]
replace_dataset(spark.createDataFrame(rule_data, rule_schema), "Rulebook")

cons_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "ConstraintId", "Scope", "TargetColumn",
     "ConstraintType", "Params", "Action", "Severity", "Description"]])
replace_dataset(spark.createDataFrame(
    [(DATASET, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7])
     for r in sensor_constraints], cons_schema), "CleaningConstraints")

print("  UnitConversionLookup : no SensorData rows (not applicable)")
print("  TranslationGlossary  : no SensorData rows (no Dutch text columns)")
print("  CodeMappings         : no SensorData rows (no coded fields)")


# Review
for t in ["Rulebook", "CleaningConstraints"]:
    print(f"===== {t} ({DATASET}) =====")
    display(spark.table(f"{SILVER_SCHEMA}.{t}").where(f"Dataset='{DATASET}'"))