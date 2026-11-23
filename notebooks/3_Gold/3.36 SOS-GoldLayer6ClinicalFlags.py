# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "senseofsensors_catalog")

CATALOG      = dbutils.widgets.get("catalog")
GOLD_SCHEMA  = f"{CATALOG}.gold"
SILVER_SCHEMA = f"{CATALOG}.silver"

LW_TABLE  = f"{GOLD_SCHEMA}.LactationWindows"
OUT_TABLE = f"{GOLD_SCHEMA}.ClinicalFlags"

print("Reading from :", LW_TABLE)
print("Writing to   :", OUT_TABLE)

# COMMAND ----------

lw = spark.table(LW_TABLE)

glossary = (spark.table(f"{SILVER_SCHEMA}.translationglossary")
    .where(F.col("Dataset") == "ReferenceData"))

print(f"LactationWindows rows : {lw.count():,}")
print(f"Glossary terms        : {glossary.count():,}")
print()
print("Glossary sample:")
display(glossary.limit(10))

# COMMAND ----------

# Understand what columns we have to work with
print("Glossary columns:")
glossary.printSchema()

print("\nDistinct FlagColumn values:")
display(glossary
    .select("FlagColumn")
    .distinct()
    .orderBy("FlagColumn"))

print("\nDistinct ApplicableColumns values:")
display(glossary
    .select("ApplicableColumns")
    .distinct()
    .orderBy("ApplicableColumns"))

# COMMAND ----------

print("Text columns available in LactationWindows:")
lw = spark.table("senseofsensors_catalog.gold.LactationWindows")
text_cols = [c for c in lw.columns if any(x in c for x in 
    ["Note", "Attention", "Overflow"])]
print(text_cols)

# COMMAND ----------

# Filter out null/empty FlagColumn before building flags
flag_terms = (glossary
    .where(F.col("FlagColumn").isNotNull() & (F.col("FlagColumn") != ""))
    .groupBy("FlagColumn")
    .agg(F.collect_list("TermNl").alias("Terms"))
    .collect())

print(f"Distinct flags to create: {len(flag_terms)}")
for row in sorted(flag_terms, key=lambda x: x.FlagColumn):
    print(f"  {row.FlagColumn}: {row.Terms[:3]}{'...' if len(row.Terms) > 3 else ''}")

# COMMAND ----------

# Start with identity columns from LactationWindows
clinical_flags = lw.select(
    "HerdIdentifier",
    "AnimalIdentifier",
    "LifeNumber",
    "LifeNumberDigits",
    "Parity",
    "LactationStart",
    "LactationEnd",
    "Notes",
    "IndividualAttention1",
    "IndividualAttention2",
    "IndividualAttention3",
    "IndividualAttention4",
    "ClinicalObservationsOverflow"
)

print(f"Starting rows: {clinical_flags.count():,}")

TEXT_COLS = [
    "Notes",
    "IndividualAttention1",
    "IndividualAttention2",
    "IndividualAttention3",
    "IndividualAttention4",
    "ClinicalObservationsOverflow"
]

for row in sorted(flag_terms, key=lambda x: x.FlagColumn):
    flag_col = row.FlagColumn
    terms    = [t.lower().strip() for t in row.Terms if t and t.strip()]

    if not terms:
        continue

    pattern = "|".join(terms)

    flag_expr = F.lit(False)
    for col_name in TEXT_COLS:
        flag_expr = flag_expr | (
            F.col(col_name).isNotNull() &
            F.lower(F.col(col_name)).rlike(pattern)
        )

    clinical_flags = clinical_flags.withColumn(flag_col, flag_expr)

print(f"Flag columns added : {len(flag_terms)}")
print(f"Total rows         : {clinical_flags.count():,}")

# COMMAND ----------

print(spark.table("senseofsensors_catalog.gold.LactationWindows").count())

# COMMAND ----------

print("Flag counts across all lactations:")
flag_cols = [row.FlagColumn for row in flag_terms]

display(clinical_flags.select(
    *[F.sum(F.col(fc).cast("int")).alias(fc)
      for fc in sorted(flag_cols)]
))

print(f"\nTotal lactations : {clinical_flags.count():,}")
print(f"Lactations with at least one flag:")
any_flag = F.lit(False)
for fc in flag_cols:
    any_flag = any_flag | F.col(fc)
print(f"  {clinical_flags.where(any_flag).count():,}")

print("\nSample flagged rows:")
display(clinical_flags
    .where(any_flag)
    .select("HerdIdentifier", "AnimalIdentifier", "Parity",
            "LactationStart", "Notes", *sorted(flag_cols))
    .limit(10))

# COMMAND ----------

(clinical_flags
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUT_TABLE))

rows = spark.table(OUT_TABLE).count()
print(f"WROTE {rows:,} rows -> {OUT_TABLE}")

# COMMAND ----------

gold_cf = spark.table(OUT_TABLE)

print("=" * 55)
print("  Gold 5 — ClinicalFlags Summary")
print("=" * 55)
print(f"  Total rows        : {gold_cf.count():>10,}")
print(f"  Distinct animals  : {gold_cf.select('AnimalIdentifier').distinct().count():>10,}")
print(f"  Distinct farms    : {gold_cf.select('HerdIdentifier').distinct().count():>10,}")
print(f"  Flag columns      : {len(flag_cols):>10,}")
print()
print("  Flag distribution per farm:")
display(gold_cf
    .groupBy("HerdIdentifier")
    .agg(*[F.sum(F.col(fc).cast("int")).alias(fc)
           for fc in sorted(flag_cols)])
    .orderBy("HerdIdentifier"))