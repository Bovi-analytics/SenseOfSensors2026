# Databricks notebook source
# Databricks notebook source
# SOS TemperatureHumidityIndex — Rulebook

dbutils.widgets.text("catalog", "senseofsensors_catalog")
CATALOG       = dbutils.widgets.get("catalog").strip()
SILVER_SCHEMA = f"{CATALOG}.silver"
DATASET       = "TemperatureHumidityIndex"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
print("Silver schema ready:", SILVER_SCHEMA)

# Shared timestamp formats tried in order.
# Source uses slash-date with space separator: "2017/01/01 00:00:00"
# Single-digit hour variant also present: "2017/01/01 4:02:46"
_TS_FMTS = (
    "yyyy/MM/dd HH:mm:ss"
    "|yyyy/MM/dd H:mm:ss"
    "|yyyy-MM-dd HH:mm:ss"
    "|yyyy-MM-dd H:mm:ss"
)

# Column rulebook
# Tuple layout (17 values):
#   0  SourceColumn      — bronze column name; None for derived columns
#   1  TargetColumn      — silver PascalCase name
#   2  DataType          — Spark type string
#   3  IsRequired        — bool
#   4  MinValue          — numeric lower bound or None
#   5  MaxValue          — numeric upper bound or None
#   6  CanonicalUnit
#   7  AllowedUnits
#   8  SemanticGroup
#   9  Description
#  10  CleaningStrategy  — semicolon-separated tokens
#  11  KeepRaw           — always False
#  12  AcceptedFormats
#  13  InvalidAction
#  14  WarningType
#  15  Pattern
#  16  AllowedValues

reference_rules = [

    # Identity
    # HerdIdentifier: 8 known farm IDs, same set as ReferenceData.
    # Leading space present in source (e.g. " 1234") — stripped by Trim.
    ('HerdIdentifier', 'HerdIdentifier', 'string', True, None, None, None, None,
     'Identity',
     'Farm / herd identifier. Same 8 farms as ReferenceData. '
     'Leading space stripped. Stored as string.',
     'Trim;NormalizeToString',
     False, None, 'Warn', 'InvalidHerdIdentifierFormat', None, None),

    # HerdName: farm name string (e.g. "FarmName"). Leading space present in source.
    # Kept in silver for human readability. Not used as a join key.
    ('HerdName', 'HerdName', 'string', False, None, None, None, None,
     'Identity',
     'Farm name corresponding to HerdIdentifier (e.g. "FarmName"). '
     'Leading space stripped. Not a join key — use HerdIdentifier for joins.',
     'Trim;NormalizeSpaces',
     False, None, None, None, None, None),

    # MeasurementDatetime: full timestamp, native Spark timestamp type.
    # Source format: "2017/01/01 00:00:00" and "2017/01/01 4:02:46" (single-digit hour).
    # Silver standard: timestamp type for date+time columns; date type for date-only columns.
    # Hourly readings: 2017-01-01 through 2020-12-31.
    ('Datetime', 'MeasurementDatetime', 'timestamp', True, None, None, None, None,
     'Timing',
     'Full measurement timestamp (hourly resolution). '
     'Source formats: yyyy/MM/dd HH:mm:ss and yyyy/MM/dd H:mm:ss (single-digit hour). '
     'Stored as native Spark timestamp. '
     'Unparseable values -> NULL + InvalidTimestampFormat warning.',
     'MultiTimestampParse',
     False, _TS_FMTS, 'Warn', 'InvalidTimestampFormat|FutureTimestamp|OldTimestamp', None, None),

    # MeasurementDate: derived date part of MeasurementDatetime.
    # Added in silver for easier daily aggregation in gold.
    (None, 'MeasurementDate', 'date', False, None, None, None, None,
     'Timing',
     'Date part of MeasurementDatetime. Derived in silver. '
     'NULL when MeasurementDatetime is NULL.',
     'DerivedDatePart',
     False, None, None, None, None, None),

    # MeasurementHour: derived hour (0-23) from MeasurementDatetime.
    # Added in silver for easier hourly pattern analysis in gold.
    (None, 'MeasurementHour', 'int', False, 0, 23, None, None,
     'Timing',
     'Hour of day (0-23) extracted from MeasurementDatetime. Derived in silver. '
     'NULL when MeasurementDatetime is NULL.',
     'DerivedHourPart',
     False, None, None, None, None, None),

    # Temperature columns: European decimal comma -> dot. Cast to double.
    # Internal: barn/indoor sensor. Mostly null for farms with external-only sensors.
    # Plausible indoor barn range: 0 to 35 C.
    ('TemperatureIntern', 'TempInternalCelsius', 'double', False, -10, 50, 'Celsius', 'Celsius',
     'Temperature',
     'Indoor / barn air temperature in degrees Celsius. '
     'European decimal comma normalised to dot. '
     'Plausible range: -10 to 50 C. Values outside range -> NULL + OutOfExpectedRange warning. '
     'Null for farms with external-only sensors.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # External: outdoor sensor. Wider range to cover Dutch winter/summer extremes.
    # Plausible outdoor range: -30 to 50 C.
    ('TemperatureExtern', 'TempExternalCelsius', 'double', False, -30, 50, 'Celsius', 'Celsius',
     'Temperature',
     'Outdoor air temperature in degrees Celsius. '
     'European decimal comma normalised to dot. '
     'Plausible range: -30 to 50 C. Values outside range -> NULL + OutOfExpectedRange warning.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # Humidity columns: percentage 0-100. European decimal comma -> dot.
    # Internal: some values are decimals (e.g. 0.988, 85.07) from sensor calibration output.
    ('RelativeHumidityIntern', 'HumidityInternalPct', 'double', False, 0, 100, 'Percent', 'Percent',
     'Humidity',
     'Indoor relative humidity as a percentage (0-100). '
     'European decimal comma normalised to dot. '
     'Sensor may output decimal values (e.g. 0.988, 85.07) — these are valid readings. '
     'Values outside 0-100 -> NULL + OutOfExpectedRange warning.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    ('RelativeHumidityExtern', 'HumidityExternalPct', 'double', False, 0, 100, 'Percent', 'Percent',
     'Humidity',
     'Outdoor relative humidity as a percentage (0-100). '
     'European decimal comma normalised to dot. '
     'Values outside 0-100 -> NULL + OutOfExpectedRange warning.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric', None, None),

    # THI columns: Temperature-Humidity Index.
    # Formula (Ravagnolo): THI = (1.8*T + 32) - (0.55 - 0.0055*RH) * (1.8*T - 26)
    # Formula verified against source data: T=0.9, RH=95 -> THI=34.29
    # Plausible range: 0-100. European decimal comma -> dot.
    # Formula consistency check declared in CleaningConstraints (tolerance ±0.5).
    ('THIIntern', 'THIInternal', 'double', False, 0, 100, 'THI', 'THI',
     'THI',
     'Indoor Temperature-Humidity Index. '
     'Computed by farm system using Ravagnolo formula: '
     'THI = (1.8*T + 32) - (0.55 - 0.0055*RH) * (1.8*T - 26). '
     'European decimal comma normalised to dot. '
     'Formula consistency checked against TempInternalCelsius and HumidityInternalPct '
     'in CleaningConstraints (tolerance ±0.5). '
     'Null for farms with external-only sensors.',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric|THIFormulaMismatch', None, None),

    ('THIExtern', 'THIExternal', 'double', False, 0, 100, 'THI', 'THI',
     'THI',
     'Outdoor Temperature-Humidity Index. '
     'Computed by farm system using Ravagnolo formula: '
     'THI = (1.8*T + 32) - (0.55 - 0.0055*RH) * (1.8*T - 26). '
     'European decimal comma normalised to dot. '
     'Formula consistency checked against TempExternalCelsius and HumidityExternalPct '
     'in CleaningConstraints (tolerance ±0.5).',
     'CommaToDot;DoubleCast',
     False, None, 'Warn', 'OutOfExpectedRange|InvalidNumeric|THIFormulaMismatch', None, None),

    # THIExternalStatus: derived heat stress classification from THIExternal.
    # Thresholds from dairy literature:
    #   < 50  -> NoStress
    #   50-72 -> Mild
    #   72-80 -> HeatStress
    #   > 80  -> SevereHeatStress
    # NULL when THIExternal is NULL.
    (None, 'THIExternalStatus', 'string', False, None, None, None, None,
     'THI',
     'Heat stress classification derived from THIExternal. '
     'Thresholds: <50=NoStress, 50-72=Mild, 72-80=HeatStress, >80=SevereHeatStress. '
     'Based on standard dairy literature thresholds. '
     'NULL when THIExternal is NULL.',
     'DerivedTHIStatus',
     False, None, None, None, None, 'NoStress|Mild|HeatStress|SevereHeatStress'),

    # THIInternalStatus: same classification from THIInternal.
    # NULL when THIInternal is NULL (expected for external-only farms).
    (None, 'THIInternalStatus', 'string', False, None, None, None, None,
     'THI',
     'Heat stress classification derived from THIInternal. '
     'Same thresholds as THIExternalStatus. '
     'NULL when THIInternal is NULL.',
     'DerivedTHIStatus',
     False, None, None, None, None, 'NoStress|Mild|HeatStress|SevereHeatStress'),
]
print(f"{DATASET}: {len(reference_rules)} column rules")


# Cleaning constraints
thi_constraints = [

    # Column-level fixes
    ('CF01', 'Column', '*',
     'TrimWhitespace', '',
     'Fix', '', 'Trim leading/trailing whitespace on all string columns'),

    ('CF02', 'Column', '*',
     'BlankToNull', 'Tokens=|Null|NULL|null|NA|N/A|NaN|nan|-|.|?| ',
     'Fix', '', 'Map empty-ish strings and lone whitespace to NULL'),

    ('CF03', 'Column', 'HerdIdentifier',
     'RejectValues', 'Values=0|00|000|NA|N/A|None|NONE|NULL|null',
     'Fix', 'Warning', 'Placeholder identifiers -> NULL + InvalidIdentifier warning'),

    # Row-level cross-field checks
    # THI formula consistency: external
    # THI = (1.8*T + 32) - (0.55 - 0.0055*RH) * (1.8*T - 26), tolerance ±0.5
    ('RC01', 'Row', 'THIExternal|TempExternalCelsius|HumidityExternalPct',
     'THIFormulaCheck',
     'Thi=THIExternal;Temp=TempExternalCelsius;Humidity=HumidityExternalPct;Tol=0.5',
     'Flag', 'Warning',
     'THIExternal should match Ravagnolo formula within ±0.5. '
     'Mismatch -> THIFormulaMismatch warning (value kept).'),

    # THI formula consistency: internal (only checked when all three are non-null)
    ('RC02', 'Row', 'THIInternal|TempInternalCelsius|HumidityInternalPct',
     'THIFormulaCheck',
     'Thi=THIInternal;Temp=TempInternalCelsius;Humidity=HumidityInternalPct;Tol=0.5',
     'Flag', 'Warning',
     'THIInternal should match Ravagnolo formula within ±0.5. '
     'Mismatch -> THIFormulaMismatch warning (value kept).'),

    # Deduplication: one reading per farm per timestamp
    ('DK01', 'Row', 'HerdIdentifier|MeasurementDatetime',
     'DedupExact', 'Keys=HerdIdentifier|MeasurementDatetime',
     'Fix', '',
     'Remove exact duplicates on HerdIdentifier + MeasurementDatetime. '
     'Keep most-complete row. Surviving row receives DuplicatesRemoved warning.'),
]
print(f"{DATASET}: {len(thi_constraints)} constraints")


# Persist tables
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
     r[10], r[12], r[13], r[14], r[15], r[16])
    for i, r in enumerate(reference_rules)
]
replace_dataset(spark.createDataFrame(rule_data, rule_schema), "Rulebook")

cons_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "ConstraintId", "Scope", "TargetColumn",
     "ConstraintType", "Params", "Action", "Severity", "Description"]])
replace_dataset(spark.createDataFrame(
    [(DATASET, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in thi_constraints],
    cons_schema), "CleaningConstraints")


# Review
for t in ["Rulebook", "CleaningConstraints"]:
    print(f"===== {t} =====")
    display(spark.table(f"{SILVER_SCHEMA}.{t}").where(f"Dataset='{DATASET}'").limit(30))

print("THI rulebook written to:", SILVER_SCHEMA)