# Databricks notebook source
#Validation Notebook — Replicate van Leerdam et al. findings

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from pyspark.sql import functions as F

GOLD_VOLUME = "/Volumes/senseofsensors_catalog/gold/gold_volume"

gm = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")

print(f"GoldMaster rows : {gm.count():,}")
print(f"Columns         : {len(gm.columns)}")

# COMMAND ----------

# What does DaysInMilk look like around calving?
display(gm
    .where(F.col("LactationStart").isNotNull())
    .select("DaysInMilk")
    .where(F.col("DaysInMilk").isNotNull())
    .groupBy("DaysInMilk")
    .count()
    .orderBy("DaysInMilk")
    .limit(30))

# COMMAND ----------

display(gm
    .where(F.col("LactationStart").isNotNull())
    .select(
        F.min("DaysInMilk").alias("MinDIM"),
        F.max("DaysInMilk").alias("MaxDIM"),
        F.sum(F.col("DaysInMilk").isNull().cast("int")).alias("NullDIM"),
        F.sum(F.col("DaysInMilk").isNotNull().cast("int")).alias("NotNullDIM"),
        F.sum((F.col("DaysInMilk") < 0).cast("int")).alias("NegativeDIM"),
    ))

# COMMAND ----------

# Paper uses 21 days before calving (DIM -21 to -1)
# Our DaysInMilk: negative = pre-calving, 0 = calving day

precalving = (gm
    .where(
        F.col("DaysInMilk").between(-21, -1) &
        F.col("SubclinicalHypocalcaemia").isNotNull() &
        F.col("LactationStart").isNotNull()
    )
    .select(
        "HerdIdentifier",
        "AnimalIdentifier",
        "Parity",
        "LactationStart",
        "EventDay",
        "DaysInMilk",
        "SubclinicalHypocalcaemia",
        "CalciumLevel",
        "DailyEatingMin15",
        "DailyRuminatingMin15",
        "DailyLyingMin15",
        "DailyStandingMin15",
        "DailyWalkingMin15",
    ))

# Full dataset
total_cows_full = precalving.select(
    "HerdIdentifier", "AnimalIdentifier", "LactationStart"
).distinct().count()

hypo_full = precalving.where(
    F.col("SubclinicalHypocalcaemia") == True
).select("HerdIdentifier", "AnimalIdentifier", "LactationStart").distinct().count()

normo_full = precalving.where(
    F.col("SubclinicalHypocalcaemia") == False
).select("HerdIdentifier", "AnimalIdentifier", "LactationStart").distinct().count()

print("=" * 50)
print("  Full dataset")
print("=" * 50)
print(f"  Total cow-lactations : {total_cows_full:,}")
print(f"  Hypocalcaemic        : {hypo_full:,}  ({hypo_full/total_cows_full*100:.1f}%)")
print(f"  Normocalcaemic       : {normo_full:,}  ({normo_full/total_cows_full*100:.1f}%)")

# Paper date range: November 2016 to May 2018
precalving_filtered = precalving.where(
    (F.col("LactationStart") >= F.lit("2016-11-01")) &
    (F.col("LactationStart") <= F.lit("2018-05-31"))
)

total_cows_filt = precalving_filtered.select(
    "HerdIdentifier", "AnimalIdentifier", "LactationStart"
).distinct().count()

hypo_filt = precalving_filtered.where(
    F.col("SubclinicalHypocalcaemia") == True
).select("HerdIdentifier", "AnimalIdentifier", "LactationStart").distinct().count()

normo_filt = precalving_filtered.where(
    F.col("SubclinicalHypocalcaemia") == False
).select("HerdIdentifier", "AnimalIdentifier", "LactationStart").distinct().count()

print()
print("=" * 50)
print("  Filtered (Nov 2016 — May 2018)")
print("=" * 50)
print(f"  Total cow-lactations : {total_cows_filt:,}")
print(f"  Hypocalcaemic        : {hypo_filt:,}  ({hypo_filt/total_cows_filt*100:.1f}%)")
print(f"  Normocalcaemic       : {normo_filt:,}  ({normo_filt/total_cows_filt*100:.1f}%)")
print()
print("  Paper reference: 609 cows, ~26.3% hypocalcaemic")

# COMMAND ----------

BEHAVIOURS = {
    "DailyEatingMin15":    "Eating",
    "DailyRuminatingMin15":"Ruminating",
    "DailyLyingMin15":     "Lying",
    "DailyStandingMin15":  "Standing",
    "DailyWalkingMin15":   "Walking",
}

def aggregate_behaviour(df, label):
    return (df
        .groupBy("DaysInMilk", "SubclinicalHypocalcaemia")
        .agg(*[
            F.round(F.avg(col), 2).alias(alias)
            for col, alias in BEHAVIOURS.items()
        ])
        .orderBy("DaysInMilk", "SubclinicalHypocalcaemia")
        .toPandas())

print("Aggregating full dataset...")
agg_full = aggregate_behaviour(precalving, "full")

print("Aggregating filtered dataset...")
agg_filtered = aggregate_behaviour(precalving_filtered, "filtered")

print(f"Full    : {len(agg_full)} rows")
print(f"Filtered: {len(agg_filtered)} rows")
display(agg_full.head(10))

# COMMAND ----------

fig, axes = plt.subplots(
    nrows=5, ncols=2,
    figsize=(16, 20),
    sharex=True
)

fig.suptitle(
    "Pre-calving Behaviour by Calcium Status\n"
    "Replication of van Leerdam et al. findings\n"
    "Left: Full dataset | Right: Paper date range (Nov 2016 – May 2018)",
    fontsize=14, fontweight="bold", y=1.01
)

COLOURS = {True: "#D62728", False: "#1F77B4"}  # red=hypo, blue=normo
LABELS  = {True: "Hypocalcaemic (Ca ≤ 2.0)", False: "Normocalcaemic (Ca > 2.0)"}

for row_idx, (col, beh_name) in enumerate(BEHAVIOURS.items()):
    for col_idx, (agg_df, dataset_label) in enumerate([
        (agg_full,     "Full dataset"),
        (agg_filtered, "Nov 2016 – May 2018")
    ]):
        ax = axes[row_idx][col_idx]

        for hypo_val in [False, True]:
            subset = agg_df[agg_df["SubclinicalHypocalcaemia"] == hypo_val]
            if not subset.empty:
                ax.plot(
                    subset["DaysInMilk"],
                    subset[beh_name],
                    color=COLOURS[hypo_val],
                    label=LABELS[hypo_val],
                    linewidth=2,
                    marker="o",
                    markersize=3
                )

        ax.set_title(f"{beh_name} — {dataset_label}", fontsize=11)
        ax.set_ylabel("Minutes per day")
        ax.set_xlabel("Days in Milk (negative = pre-calving)")
        ax.axvline(x=0, color="gray", linestyle="--", alpha=0.5, label="Calving")
        ax.invert_xaxis()  # show -21 on left, 0 on right (closer to calving)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

plt.tight_layout()
plt.savefig("/tmp/validation_precalving_behaviour.png", dpi=150, bbox_inches="tight")
plt.show()
print("Plot saved to /tmp/validation_precalving_behaviour.png")

# COMMAND ----------

# Paper key findings to validate:
# 1. Normocalcaemic cows spent MORE time eating than hypocalcaemic
# 2. Normocalcaemic cows spent MORE time walking than hypocalcaemic
# 3. Hypocalcaemic cows spent MORE time standing until day 5 before calving
# 4. Difference in eating increased closer to parturition

print("=" * 65)
print("  Validation against van Leerdam et al. key findings")
print("=" * 65)

for dataset_label, agg_df in [("Full", agg_full), ("Filtered", agg_filtered)]:
    print(f"\n  --- {dataset_label} dataset ---")

    normo = agg_df[agg_df["SubclinicalHypocalcaemia"] == False]
    hypo  = agg_df[agg_df["SubclinicalHypocalcaemia"] == True]

    # Finding 1: Eating — normo > hypo overall
    normo_eat = normo["Eating"].mean()
    hypo_eat  = hypo["Eating"].mean()
    finding1  = "✅" if normo_eat > hypo_eat else "❌"
    print(f"\n  {finding1} Finding 1 — Normo eats more than hypo overall")
    print(f"     Normo avg eating: {normo_eat:.1f} min/day")
    print(f"     Hypo  avg eating: {hypo_eat:.1f} min/day")
    print(f"     Difference      : {normo_eat - hypo_eat:.1f} min/day")

    # Finding 2: Walking — normo > hypo overall
    normo_walk = normo["Walking"].mean()
    hypo_walk  = hypo["Walking"].mean()
    finding2   = "✅" if normo_walk > hypo_walk else "❌"
    print(f"\n  {finding2} Finding 2 — Normo walks more than hypo overall")
    print(f"     Normo avg walking: {normo_walk:.1f} min/day")
    print(f"     Hypo  avg walking: {hypo_walk:.1f} min/day")

    # Finding 3: Standing — hypo > normo until day -5
    normo_stand_early = normo[normo["DaysInMilk"] >= -5]["Standing"].mean()
    hypo_stand_early  = hypo[hypo["DaysInMilk"]   >= -5]["Standing"].mean()
    finding3          = "✅" if hypo_stand_early > normo_stand_early else "❌"
    print(f"\n  {finding3} Finding 3 — Hypo stands more than normo in last 5 days")
    print(f"     Normo avg standing (DIM -5 to -1): {normo_stand_early:.1f} min/day")
    print(f"     Hypo  avg standing (DIM -5 to -1): {hypo_stand_early:.1f} min/day")

    # Finding 4: Eating difference increases closer to calving
    eat_diff_early = (
        normo[normo["DaysInMilk"].between(-21, -11)]["Eating"].mean() -
        hypo[hypo["DaysInMilk"].between(-21, -11)]["Eating"].mean()
    )
    eat_diff_late = (
        normo[normo["DaysInMilk"].between(-10, -1)]["Eating"].mean() -
        hypo[hypo["DaysInMilk"].between(-10, -1)]["Eating"].mean()
    )
    finding4 = "✅" if eat_diff_late > eat_diff_early else "❌"
    print(f"\n  {finding4} Finding 4 — Eating difference increases closer to calving")
    print(f"     Normo-Hypo eating diff (DIM -21 to -11): {eat_diff_early:.1f} min/day")
    print(f"     Normo-Hypo eating diff (DIM -10 to  -1): {eat_diff_late:.1f} min/day")

print()
print("=" * 65)
print("  Paper reference values (Fig 7, van Leerdam et al.):")
print("  Normo eating ~280-320 min/day, Hypo eating ~240-280 min/day")
print("  Walking difference ~5-15 min/day in favour of normo")
print("  Standing: hypo > normo until DIM -5")
print("=" * 65)