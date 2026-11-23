# Databricks notebook source
dbutils.widgets.text("catalog", "senseofsensors_catalog")
CATALOG       = dbutils.widgets.get("catalog").strip()
SILVER_SCHEMA = f"{CATALOG}.silver"
DATASET       = "MilkYield"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# Column rulebook — same 17-value layout as the other rulebooks:
#   0 SourceColumn  1 TargetColumn  2 DataType  3 IsRequired  4 MinValue  5 MaxValue  6 CanonicalUnit
#   7 AllowedUnits  8 SemanticGroup  9 Description  10 CleaningStrategy  11 KeepRaw  12 AcceptedFormats
#   13 InvalidAction  14 WarningType  15 Pattern  16 AllowedValues
milk_rules = [
    # Identity
    (None, 'HerdIdentifier', 'string', True, None, None, None, None, 'Identity',
     'Farm identifier. The workbook has one sheet per farm; the farm is assigned in silver by matching the '
     'cows\' life numbers with the other datasets, then anonymised to Farm_A … Farm_H.',
     'DerivedFromSheet', False, None, 'Fail', 'UnmatchedFarm', None, None),
    ('Usernumber', 'AnimalIdentifier', 'string', True, None, None, None, None, 'Identity',
     'Farm cow number. Cow numbers are reused by other cows over time, so join on LifeNumberDigits. '
     'Pseudonymised (hash of farm + cow number).',
     'Trim;NormalizeToString', False, None, 'Warn', 'MissingRequired', None, None),
    ('Life number', 'LifeNumberDigits', 'string', False, None, None, None, None, 'Identity',
     'National life number. Pseudonymised to the same digits-only hash used in every other dataset, '
     'so it joins with LifeNumberDigits / BestLifeNumberDigits in gold.',
     'Trim;UpperCase', False, None, 'Warn', 'MissingLifeNumber', r'^[A-Z]{2}\s?\d{6,}$', None),

    # Lactation
    ('Lactation number', 'LactationNumber', 'int', False, 1, 20, 'count', 'count', 'Lactation',
     'Lactation number (parity) of the cow.', 'IntCast', False, None, 'Warn', 'OutOfExpectedRange', None, None),
    ('Calving date', 'CalvingDate', 'date', False, None, None, None, None, 'Lactation',
     'Calving date that started this lactation.', 'ParseDate', False, 'yyyy-MM-dd HH:mm:ss|yyyy-MM-dd',
     'Warn', 'InvalidDate', None, None),
    ('Dry off date', 'PreviousDryOffDate', 'timestamp', False, None, None, None, None, 'Lactation',
     'Dry-off before this calving (it always falls before CalvingDate, so it belongs to the previous lactation). '
     'Empty for first-calving cows and when not recorded.',
     'ParseTimestamp', False, 'yyyy-MM-dd HH:mm:ss|yyyy-MM-dd', 'Warn', 'InvalidDate|DryOffAfterCalving', None, None),
    (None, 'DryPeriodDays', 'int', False, 0, 200, 'd', 'd', 'Lactation',
     'Days between PreviousDryOffDate and CalvingDate.', 'Derived', False, None, 'Warn', 'OutOfExpectedRange',
     None, None),

    # Day
    ('Date', 'EventDay', 'date', True, None, None, None, None, 'Time',
     'Calendar day of the milk record.', 'ParseDate', False, 'yyyy-MM-dd HH:mm:ss|yyyy-MM-dd',
     'Warn', 'InvalidDate', None, None),
    ('Lactation day', 'DaysInMilk', 'int', False, 0, 400, 'd', 'd', 'Time',
     'Days since calving as recorded by the milking system (EventDay − CalvingDate). The export covers days 0–105.',
     'IntCast', False, None, 'Warn', 'OutOfExpectedRange|DaysInMilkMismatch', None, None),

    # Milk
    ('Day production', 'DailyMilkKg', 'double', False, 0, 80, 'kg/d', 'kg/d', 'Milk',
     'Milk yield for the day. 0 kg is kept in silver with a warning and treated as missing in gold.',
     'DoubleCast', False, None, 'Warn', 'OutOfExpectedRange|ZeroValue', None, None),
    ('Fat percentage', 'MilkFatPct', 'double', False, 1.5, 9, '%', '%', 'Milk',
     'Milk fat content. 0 means not measured and is set to null. Sparse on some farms.',
     'DoubleCast;ZeroToNull', False, None, 'Warn', 'OutOfExpectedRange|ZeroAsMissing', None, None),
    ('Protein percentage', 'MilkProteinPct', 'double', False, 1.5, 7, '%', '%', 'Milk',
     'Milk protein content. 0 means not measured and is set to null. Sparse on some farms.',
     'DoubleCast;ZeroToNull', False, None, 'Warn', 'OutOfExpectedRange|ZeroAsMissing', None, None),
]
print(f"{DATASET}: {len(milk_rules)} column rules")

milk_constraints = [
    ('CF01', 'Column', '*', 'TrimWhitespace', '', 'Fix', '', 'Trim leading/trailing whitespace on all string columns'),
    ('CF02', 'Column', '*', 'BlankToNull', 'Tokens=|NA|N/A|nan|NaN|NaT|None|null|NULL|-',
     'Fix', '', 'Map empty-ish strings to NULL'),
    ('CF03', 'Column', 'MilkFatPct|MilkProteinPct', 'ZeroToNull', '', 'Fix', 'Warning',
     '0 % means not measured -> NULL + ZeroAsMissing warning'),
    ('RC01', 'Row', 'DaysInMilk|EventDay|CalvingDate', 'DateDifferenceCheck', 'Expected=EventDay-CalvingDate;Tol=0',
     'Flag', 'Warning', 'DaysInMilk should equal EventDay − CalvingDate -> DaysInMilkMismatch warning (kept)'),
    ('RC02', 'Row', 'PreviousDryOffDate|CalvingDate', 'OrderCheck', 'Before=PreviousDryOffDate;After=CalvingDate',
     'Flag', 'Warning', 'Dry-off must fall before calving -> DryOffAfterCalving warning (kept)'),
    ('RC03', 'Row', 'LifeNumberDigits', 'RequiredForJoin', '', 'Flag', 'Warning',
     'Rows without a life number cannot be linked to the other datasets -> MissingLifeNumber warning (kept in '
     'silver, left out of gold)'),
    ('DK01', 'Row', '*', 'DedupExact', 'Keys=all columns', 'Fix', '',
     'Remove exact duplicate rows (same sheet, same values in every column).'),
    ('DK02', 'Row', 'HerdIdentifier|LifeNumberDigits|EventDay', 'FlagRepeated',
     'Keys=HerdIdentifier|LifeNumberDigits|EventDay', 'Flag', 'Warning',
     'The system sometimes stores the same cow-day twice with near-identical yields (a repeated record, not a '
     'second milking: their mean matches neighbouring days, their sum does not). Kept in silver with '
     'RepeatedDayRecord warning; averaged in gold.'),
    ('FM01', 'Dataset', 'HerdIdentifier', 'FarmByLifeNumberOverlap', 'MinOverlap=0.3',
     'Fail', 'Error', 'Each sheet is assigned to the farm whose cows share the most life numbers with it; the '
     'notebook stops if the best overlap is below 30 % or two sheets map to the same farm.'),
]
print(f"{DATASET}: {len(milk_constraints)} constraints")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

def replace_dataset(df, table):
    fq = f"{SILVER_SCHEMA}.{table}"
    if spark.catalog.tableExists(fq):
        spark.sql(f"DELETE FROM {fq} WHERE Dataset = '{DATASET}'")
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(fq)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)
    print(f"  {table}: {df.count()} rows for {DATASET}")

RULE_COLS = ["SourceColumn", "TargetColumn", "DataType", "IsRequired", "MinValue", "MaxValue", "CanonicalUnit",
             "AllowedUnits", "SemanticGroup", "Description", "CleaningStrategy", "AcceptedFormats", "InvalidAction",
             "WarningType", "Pattern", "AllowedValues"]
rule_schema = StructType(
    [StructField("Dataset", StringType()), StructField("Ordinal", IntegerType())]
    + [StructField(c, StringType() if c not in ("IsRequired", "MinValue", "MaxValue")
                   else (BooleanType() if c == "IsRequired" else DoubleType())) for c in RULE_COLS])
rule_data = [(DATASET, i, r[0], r[1], r[2], r[3],
              None if r[4] is None else float(r[4]), None if r[5] is None else float(r[5]),
              r[6], r[7], r[8], r[9], r[10], r[12], r[13], r[14], r[15], r[16])
             for i, r in enumerate(milk_rules)]
replace_dataset(spark.createDataFrame(rule_data, rule_schema), "Rulebook")

cons_schema = StructType([StructField(n, StringType()) for n in
    ["Dataset", "ConstraintId", "Scope", "TargetColumn", "ConstraintType", "Params", "Action", "Severity",
     "Description"]])
replace_dataset(spark.createDataFrame([(DATASET, *r) for r in milk_constraints], cons_schema), "CleaningConstraints")

for t in ["Rulebook", "CleaningConstraints"]:
    print(f"===== {t} =====")
    display(spark.table(f"{SILVER_SCHEMA}.{t}").where(f"Dataset='{DATASET}'"))