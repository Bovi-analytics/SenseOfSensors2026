# Databricks notebook source
## CELL 1 — Imports and data loading

from pyspark.sql import functions as F
import json, shutil, os

dbutils.widgets.text("RepoUrl", "https://github.com/Bovi-analytics/SenseOfSensors2026")
dbutils.widgets.text("DatasetDoi", "")      # e.g. 10.xxxx/yyyy once the dataset is published
dbutils.widgets.text("BovidaetaUrl", "")    # link to the dataset in bovidaeta, once published
REPO_URL    = dbutils.widgets.get("RepoUrl").strip().rstrip("/")
DATASET_DOI = dbutils.widgets.get("DatasetDoi").strip()
BOVIDAETA_URL = dbutils.widgets.get("BovidaetaUrl").strip()
 
GOLD_VOLUME   = "/Volumes/senseofsensors_catalog/gold/gold_volume"
SILVER_VOLUME = "/Volumes/senseofsensors_catalog/silver/silver_volume"
SILVER_SCHEMA = "senseofsensors_catalog.silver"
 
gm   = spark.read.format("delta").load(f"{GOLD_VOLUME}/GoldMaster")
lw   = spark.read.format("delta").load(f"{GOLD_VOLUME}/LactationWindows")
dbs  = spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyBehaviourSummary")
thi  = spark.read.format("delta").load(f"{GOLD_VOLUME}/DailyTHISummary")
cf   = spark.read.format("delta").load(f"{GOLD_VOLUME}/ClinicalFlags")
rt   = spark.read.format("delta").load(f"{GOLD_VOLUME}/ReproductiveTimeline")
aib  = spark.read.format("delta").load(f"{GOLD_VOLUME}/AnimalIdentityBridge")
rb   = spark.table(f"{SILVER_SCHEMA}.Rulebook")
cal  = spark.table(f"{SILVER_SCHEMA}.calendardata")
 
print("✅ All tables loaded")
print(f"  GoldMaster      : {gm.count():,} rows")
print(f"  LactationWindows: {lw.count():,} rows")
print(f"  DailyBehaviour  : {dbs.count():,} rows")

# COMMAND ----------

## CELL 2a — Pipeline counts
counts = {
    "sensor_bronze":     1_214_421_979,
    "sensor_silver":     1_204_964_611,
    "ref_silver":        spark.read.format("delta").load(f"{SILVER_VOLUME}/ReferenceData").count(),
    "thi_silver":        spark.read.format("delta").load(f"{SILVER_VOLUME}/TemperatureHumidityIndex").count(),
    "calendar_silver":   cal.count(),
    "lactation_windows": lw.count(),
    "identity_bridge":   aib.count(),
    "daily_behaviour":   dbs.count(),
    "daily_thi":         thi.count(),
    "clinical_flags":    cf.count(),
    "repro_timeline":    rt.count(),
    "gold_master":       gm.count(),
    "farms":             8,
    "gold_columns":      len(gm.columns),
}
print(" counts done")

# COMMAND ----------

## CELL 2b — Farm stats
farm_stats = (dbs
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("AnimalDays"),
        F.countDistinct("AnimalIdentifier").alias("Animals"),
        F.countDistinct("EventDay").alias("Days"),
        F.round(F.avg(F.col("DailyEatingMin15").cast("double")), 1).alias("AvgEating"),
        F.round(F.avg(F.col("DailyLyingMin15").cast("double")), 1).alias("AvgLying"),
        F.round(F.avg(F.col("DailyRuminatingMin15").cast("double")), 1).alias("AvgRuminating"),
        F.round(F.avg(F.col("DailyWalkingMin15").cast("double")), 1).alias("AvgWalking"),
    )
    .orderBy("HerdIdentifier")
    .toPandas())
print(f" farm_stats: {len(farm_stats)} farms")
 

# COMMAND ----------

## CELL 2c — Flag counts

flag_cols = [c for c in cf.columns if c.startswith("Note") and c != "Notes"]
flag_counts = cf.select(
    *[F.sum(F.when(F.col(fc) == True, 1).otherwise(0)).alias(fc) for fc in flag_cols]
).toPandas().T.reset_index()
flag_counts.columns = ["Flag", "Count"]
flag_counts = flag_counts.sort_values("Count", ascending=False).head(20)
print(f" flag_counts: {len(flag_counts)} flags")

# COMMAND ----------

## CELL 2d — Parity behaviour

parity_beh = (gm
    .where(F.col("Parity").isNotNull() & F.col("DailyEatingMin15").isNotNull())
    .groupBy("Parity")
    .agg(
        F.round(F.avg(F.col("DailyEatingMin15").cast("double")), 1).alias("Eating"),
        F.round(F.avg(F.col("DailyLyingMin15").cast("double")), 1).alias("Lying"),
        F.round(F.avg(F.col("DailyRuminatingMin15").cast("double")), 1).alias("Ruminating"),
        F.round(F.avg(F.col("DailyWalkingMin15").cast("double")), 1).alias("Walking"),
        F.count("*").alias("Rows"),
    )
    .where(F.col("Parity") <= 8)
    .orderBy("Parity")
    .toPandas())
print(f" parity_beh: {len(parity_beh)} parities")

# COMMAND ----------

## CELL 2e — Pre-calving behaviour

precalving = (gm
    .where(
        F.col("DaysInMilk").between(-21, -1) &
        F.col("SubclinicalHypocalcaemia").isNotNull()
    )
    .groupBy("DaysInMilk", "SubclinicalHypocalcaemia")
    .agg(
        F.round(F.avg(F.col("DailyEatingMin15").cast("double")), 1).alias("Eating"),
        F.round(F.avg(F.col("DailyLyingMin15").cast("double")), 1).alias("Lying"),
        F.round(F.avg(F.col("DailyStandingMin15").cast("double")), 1).alias("Standing"),
        F.round(F.avg(F.col("DailyWalkingMin15").cast("double")), 1).alias("Walking"),
        F.round(F.avg(F.col("DailyRuminatingMin15").cast("double")), 1).alias("Ruminating"),
    )
    .orderBy("DaysInMilk", "SubclinicalHypocalcaemia")
    .toPandas())
print(f" precalving: {len(precalving)} rows")

# COMMAND ----------

## CELL 2f — Clinical rates

clinical_rates = (lw
    .groupBy("HerdIdentifier")
    .agg(
        F.count("*").alias("Total"),
        F.sum(F.when(F.col("SubclinicalKetosis")      == True, 1).otherwise(0)).alias("Ketosis"),
        F.sum(F.when(F.col("SubclinicalHypocalcaemia") == True, 1).otherwise(0)).alias("Hypocalcaemia"),
        F.sum(F.when(F.col("DifficultCalving")         == True, 1).otherwise(0)).alias("DifficultCalving"),
        F.sum(F.when(F.col("RetainedPlacenta")         == True, 1).otherwise(0)).alias("RetainedPlacenta"),
    )
    .toPandas())
clinical_rates["KetosisRate"]       = (clinical_rates["Ketosis"]       / clinical_rates["Total"] * 100).round(1)
clinical_rates["HypocalcaemiaRate"] = (clinical_rates["Hypocalcaemia"] / clinical_rates["Total"] * 100).round(1)
clinical_rates["DifficultRate"]     = (clinical_rates["DifficultCalving"] / clinical_rates["Total"] * 100).round(1)
clinical_rates["RetainedRate"]      = (clinical_rates["RetainedPlacenta"] / clinical_rates["Total"] * 100).round(1)
print(f" clinical_rates: {len(clinical_rates)} farms")

# COMMAND ----------

## CELL 2g — DIM trend

dim_trend = (gm
    .where(F.col("DaysInMilk").between(0, 100) & F.col("DailyEatingMin15").isNotNull())
    .groupBy("DaysInMilk")
    .agg(
        F.round(F.avg(F.col("DailyEatingMin15").cast("double")), 1).alias("Eating"),
        F.round(F.avg(F.col("DailyLyingMin15").cast("double")), 1).alias("Lying"),
        F.round(F.avg(F.col("DailyRuminatingMin15").cast("double")), 1).alias("Ruminating"),
        F.round(F.avg(F.col("DailyWalkingMin15").cast("double")), 1).alias("Walking"),
        F.round(F.avg(F.col("DailyStandingMin15").cast("double")), 1).alias("Standing"),
    )
    .orderBy("DaysInMilk")
    .toPandas())
print(f" dim_trend: {len(dim_trend)} days")

# COMMAND ----------

## CELL 2h — THI monthly + farm stats

thi_monthly = (thi
    .withColumn("Month", F.month("MeasurementDate"))
    .groupBy("Month")
    .agg(
        F.round(F.avg(F.col("MaxTHIExternal").cast("double")), 2).alias("AvgMaxTHI"),
        F.round(F.avg(F.col("MeanTHIExternal").cast("double")), 2).alias("AvgMeanTHI"),
        F.sum((F.col("HeatStressHoursExternal") > 0).cast("int")).alias("HeatStressDays"),
    )
    .orderBy("Month")
    .toPandas())
 
thi_stats = (thi
    .groupBy("HerdIdentifier")
    .agg(
        F.sum((F.col("HeatStressHoursExternal") > 0).cast("int")).alias("HeatStressDays"),
        F.max(F.col("MaxTHIExternal").cast("double")).alias("MaxTHI"),
        F.round(F.avg(F.col("MeanTHIExternal").cast("double")), 2).alias("MeanTHI"),
    )
    .orderBy("HerdIdentifier")
    .toPandas())
print(f" thi_monthly: {len(thi_monthly)} months")

# COMMAND ----------

## CELL 2i — Remaining stats

repro_stats = (rt
    .groupBy("HerdIdentifier")
    .agg(
        F.round(F.avg(F.col("InseminationCount").cast("double")), 2).alias("AvgInseminations"),
        F.round(F.avg(F.col("DaysOpen").cast("double")), 1).alias("AvgDaysOpen"),
        F.round(F.avg(F.col("CalvingIntervalDays").cast("double")), 1).alias("AvgCalvingInterval"),
        F.sum(F.col("ReproductiveSuccess").cast("int")).alias("ReproSuccessCount"),
        F.count("*").alias("Total"),
    )
    .toPandas())
 
bridge_stats = aib.groupBy("JoinMethod").count().toPandas()
 
total_gm = gm.count()
null_profile = gm.select(
    *[F.round(F.sum(F.col(c).isNull().cast("int")) / F.lit(total_gm) * 100, 1).alias(c)
      for c in ["DailyEatingMin15","DailyLyingMin15","DailyRuminatingMin15",
                "MaxTHIExternal","SubclinicalKetosis","CalciumLevel",
                "InseminationCount","LactationStart","Parity"]]
).toPandas().T.reset_index()
null_profile.columns = ["Column","NullPct"]
 
# Behaviour by farm — per DIM for dynamic farm filter
farm_dim = (gm
    .where(F.col("DaysInMilk").between(0,60) & F.col("DailyEatingMin15").isNotNull())
    .groupBy("HerdIdentifier","DaysInMilk")
    .agg(
        F.round(F.avg(F.col("DailyEatingMin15").cast("double")),1).alias("Eating"),
        F.round(F.avg(F.col("DailyLyingMin15").cast("double")),1).alias("Lying"),
        F.round(F.avg(F.col("DailyRuminatingMin15").cast("double")),1).alias("Ruminating"),
    )
    .orderBy("HerdIdentifier","DaysInMilk")
    .toPandas())
 
print(f" repro_stats, bridge_stats, null_profile, farm_dim done")

# COMMAND ----------

## CELL 2j — Milk recording (optional: only if gold DailyMilkYield exists)

MILK_PATH = f"{GOLD_VOLUME}/DailyMilkYield"
try:
    milk = spark.read.format("delta").load(MILK_PATH)
    HAS_MILK = milk.limit(1).count() > 0
except Exception:
    HAS_MILK = False

milk_curve, milk_beh, milk_stats = [], [], {}
if HAS_MILK:
    mk_ok = milk.where(F.col("DaysInMilk").between(0, 105) & F.col("DailyMilkKg").isNotNull())
    milk_curve = (mk_ok.groupBy("HerdIdentifier", "DaysInMilk")
                       .agg(F.round(F.avg("DailyMilkKg"), 1).alias("MeanMilkKg"))
                       .orderBy("HerdIdentifier", "DaysInMilk").toPandas())
    both = (mk_ok.where("HasSensorData")
                 .join(gm.select("HerdIdentifier", F.col("BestLifeNumberDigits").alias("LifeNumberDigits"), "EventDay",
                                 "DailyEatingMin15", "DailyRuminatingMin15", "DailyLyingMin15"),
                       ["HerdIdentifier", "LifeNumberDigits", "EventDay"]))
    milk_beh = (both.groupBy("DaysInMilk")
                    .agg(F.round(F.avg("DailyMilkKg"), 1).alias("Milk"),
                         F.round(F.avg(F.col("DailyRuminatingMin15").cast("double")), 1).alias("Ruminating"),
                         F.round(F.avg(F.col("DailyEatingMin15").cast("double")), 1).alias("Eating"),
                         F.count("*").alias("CowDays"))
                    .where("CowDays >= 20").orderBy("DaysInMilk").toPandas())
    allc = (mk_ok.groupBy("DaysInMilk").agg(F.avg("DailyMilkKg").alias("m")).orderBy(F.desc("m")).first())
    s = milk.agg(F.countDistinct("HerdIdentifier").alias("farms"), F.countDistinct("LifeNumberDigits").alias("cows"),
                 F.count("*").alias("days"), F.avg("DailyMilkKg").alias("mean"),
                 F.avg(F.col("HasSensorData").cast("int")).alias("sens"),
                 F.min("EventDay").alias("first"), F.max("EventDay").alias("last")).first()
    milk_stats = {"farms": s["farms"], "cows": s["cows"], "days": s["days"], "mean": round(s["mean"], 1),
                  "sens": round(100 * s["sens"]), "first": str(s["first"])[:4], "last": str(s["last"])[:4],
                  "peak": round(allc["m"], 1), "peak_dim": int(allc["DaysInMilk"])}
    milk_curve, milk_beh = milk_curve.to_dict(orient="records"), milk_beh.fillna(0).to_dict(orient="records")
print(f" milk: {'included — ' + str(milk_stats) if HAS_MILK else 'no DailyMilkYield table, section left out'}")

# COMMAND ----------

## CELL 3 — Build JSON

def pdf_to_json(df):
    return df.fillna(0).to_dict(orient="records")
 
data = {
    "counts":         counts,
    "farm_stats":     pdf_to_json(farm_stats),
    "thi_stats":      pdf_to_json(thi_stats),
    "flag_counts":    pdf_to_json(flag_counts),
    "parity_beh":     pdf_to_json(parity_beh),
    "precalving":     pdf_to_json(precalving),
    "repro_stats":    pdf_to_json(repro_stats),
    "clinical_rates": pdf_to_json(clinical_rates),
    "dim_trend":      pdf_to_json(dim_trend),
    "thi_monthly":    pdf_to_json(thi_monthly),
    "bridge_stats":   pdf_to_json(bridge_stats),
    "null_profile":   pdf_to_json(null_profile),
    "farm_dim":       pdf_to_json(farm_dim),
    "milk_curve":     milk_curve,
    "milk_beh":       milk_beh,
}
data_json = json.dumps(data, default=str)
print(f" JSON ready — {len(data_json):,} bytes")

# COMMAND ----------

## CELL 4a — Data & Code and milk sections

doi_html = (f'<a class="btn" href="https://doi.org/{DATASET_DOI}">Dataset DOI</a>' if DATASET_DOI
            else '<span class="pending">Dataset DOI: added on publication</span>')
milk_card_note = (f" Milk recording (MilkDay) is included for {milk_stats['farms']} farms." if HAS_MILK else "")
milk_hero = (f", with daily milk recording for {milk_stats['farms']} farms" if HAS_MILK else "")
milk_topic = ", milk yield" if HAS_MILK else ""
n_datasets = 5 if HAS_MILK else 4
gold_tables = 8 if HAS_MILK else 7
milk_bronze = (", MilkYield (Excel workbook, one sheet per farm)" if HAS_MILK else "")
bovidaeta_html = (f'<a class="btn alt" href="{BOVIDAETA_URL}">bovidaeta format</a>' if BOVIDAETA_URL else "")

access_section = f"""
<!-- DATA & CODE -->
<section id="access">
  <div class="section-eyebrow">Open Data &amp; Code</div>
  <div class="section-title">Get the Data, the Pipeline and the Documentation</div>
  <p class="section-desc">
    Two analysis-ready datasets are released from the gold layer, one folder per farm (Farm A–Farm H).
    Farm, animal and life-number identifiers are pseudonymised; raw (bronze) data is not shared.
  </p>
  <div class="grid-3">
    <div class="card access-card">
      <h3>SOS24h — daily dataset</h3>
      <p>One row per cow per day: time budgets (eating, ruminating, lying, standing, walking, inactive),
         lactation stage, health and reproduction records, and daily farm climate (THI).{milk_card_note}</p>
      {doi_html} {bovidaeta_html}
    </div>
    <div class="card access-card">
      <h3>SOS2h — 2-hour dataset</h3>
      <p>The same cows in twelve 2-hour bins per day, built from the 15-minute sensor series,
         with 2-hourly farm climate. Daily totals in SOS24h equal the sum of these bins.</p>
      {doi_html} {bovidaeta_html}
    </div>
    <div class="card access-card">
      <h3>Pipeline code</h3>
      <p>The Databricks notebooks that build bronze, silver and gold, the rulebooks that declare every
         cleaning rule, and the export that writes SOS24h and SOS2h.</p>
      <a class="btn" href="{REPO_URL}">GitHub repository</a>
      <a class="btn alt" href="{REPO_URL}/tree/main/notebooks">Notebooks</a>
      <a class="btn alt" href="{REPO_URL}#readme">README</a>
    </div>
  </div>
  <div class="licence-row">
    <strong>Licences.</strong> Data: <a href="https://creativecommons.org/licenses/by/4.0/">CC BY 4.0</a>
    (<a href="{REPO_URL}/blob/main/LICENSE-DATA.md">LICENSE-DATA</a>) · Code: MIT
    (<a href="{REPO_URL}/blob/main/LICENSE">LICENSE</a>).
    Please cite the dataset publication when you use the data.
  </div>
</section>
"""

milk_nav, milk_section = "", ""
if HAS_MILK:
    ms = milk_stats
    milk_nav = '  <a href="#milk">Milk</a>\n'
    milk_section = f"""
<!-- MILK -->
<section id="milk">
  <div class="section-eyebrow">Milk Recording</div>
  <div class="section-title">Daily Milk Yield in Early Lactation</div>
  <p class="section-desc">
    Daily milk yield from the milking systems of {ms['farms']} farms, days 0–105 of each lactation
    ({ms['first']}–{ms['last']}). Linked to the sensor data through the pseudonymised cow ID, so milk and
    behaviour can be compared for the same cow on the same day.
  </p>
  <div class="grid-4" style="margin-bottom:1.5rem">
    <div class="stat-card"><div class="stat-label">Cows with milk records</div>
      <div class="stat-value green">{ms['cows']:,}</div><div class="stat-sub">{ms['farms']} farms</div></div>
    <div class="stat-card"><div class="stat-label">Cow-days of milk recording</div>
      <div class="stat-value">{ms['days']:,}</div><div class="stat-sub">One row per cow per day</div></div>
    <div class="stat-card"><div class="stat-label">Peak of the mean lactation curve</div>
      <div class="stat-value amber">{ms['peak']} kg</div><div class="stat-sub">at day {ms['peak_dim']} in milk</div></div>
    <div class="stat-card"><div class="stat-label">Also have sensor data</div>
      <div class="stat-value green">{ms['sens']}%</div><div class="stat-sub">of milk cow-days</div></div>
  </div>
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Mean Daily Milk by Days in Milk — per Farm</div>
      <div style="position:relative;height:260px"><canvas id="milkCurveChart"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title">Milk, Rumination and Eating — Same Cow-Days</div>
      <div style="position:relative;height:260px"><canvas id="milkBehChart"></canvas></div>
    </div>
  </div>
</section>
"""
print(" sections ready")

# COMMAND ----------

## CELL 4 — Generate HTML

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>SenseOfSensors — Research Report</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Source+Serif+4:ital,wght@0,300;0,400;0,600;0,700;1,400&family=Inter:wght@300;400;500;600&display=swap');
 
:root {{
  --cream:    #faf7f2;
  --warm:     #f5efe6;
  --warm2:    #ede3d4;
  --border:   #d4c5b0;
  --brown:    #5c4a35;
  --brown2:   #8b6f47;
  --green:    #3a6b4a;
  --green2:   #5a9468;
  --green3:   #c8dfc8;
  --amber:    #c17f2a;
  --amber2:   #e8a84a;
  --amber3:   #fdf0d5;
  --red:      #b03a2e;
  --red2:     #e8a09a;
  --text:     #2d2416;
  --muted:    #7a6a57;
  --muted2:   #a89880;
  --white:    #ffffff;
  --shadow:   0 2px 12px rgba(92,74,53,0.10);
  --shadow2:  0 4px 24px rgba(92,74,53,0.14);
}}
 
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
html {{ scroll-behavior: smooth; }}
 
body {{
  background: var(--cream);
  color: var(--text);
  font-family: 'Inter', sans-serif;
  font-size: 14px;
  line-height: 1.65;
}}
 
/* ── NAV ─────────────────────────────────────────── */
nav {{
  position: sticky; top: 0; z-index: 200;
  background: rgba(250,247,242,0.96);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--border);
  display: flex; align-items: center;
  padding: 0 2rem; height: 52px; gap: 0.5rem;
}}
.nav-logo {{
  font-family: 'Source Serif 4', serif;
  font-weight: 700; font-size: 1.05rem;
  color: var(--brown); margin-right: 1.5rem;
  white-space: nowrap;
}}
.nav-logo span {{ color: var(--green); }}
nav a {{
  color: var(--muted); text-decoration: none;
  font-size: 0.78rem; font-weight: 500;
  padding: 0.3rem 0.7rem; border-radius: 4px;
  transition: all 0.15s;
}}
nav a:hover {{ background: var(--warm2); color: var(--brown); }}
.nav-spacer {{ flex: 1; }}
.nav-badge {{
  font-size: 0.72rem; padding: 0.2rem 0.6rem;
  background: var(--green3); color: var(--green);
  border-radius: 20px; font-weight: 600;
}}
 
/* ── HERO ────────────────────────────────────────── */
.hero {{
  background: linear-gradient(135deg, var(--brown) 0%, #3d2c1a 60%, var(--green) 100%);
  color: white; padding: 4rem 2rem 3.5rem;
  position: relative; overflow: hidden;
}}
.hero::before {{
  content: '';
  position: absolute; inset: 0;
  background: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.03'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E");
}}
.hero-inner {{
  max-width: 1200px; margin: 0 auto;
  display: grid; grid-template-columns: 1.4fr 1fr; gap: 4rem;
  align-items: center; position: relative;
}}
.hero-eyebrow {{
  font-size: 0.72rem; font-weight: 600; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--amber2);
  margin-bottom: 0.75rem;
}}
.hero-title {{
  font-family: 'Source Serif 4', serif;
  font-size: clamp(2rem, 3.5vw, 3rem);
  font-weight: 700; line-height: 1.15;
  color: white; margin-bottom: 1rem;
}}
.hero-title em {{
  font-style: italic; color: var(--amber2);
}}
.hero-desc {{
  color: rgba(255,255,255,0.75);
  font-size: 0.92rem; max-width: 520px;
  margin-bottom: 1.5rem; line-height: 1.7;
}}
.hero-institutions {{
  display: flex; gap: 1.5rem; flex-wrap: wrap;
  align-items: center; margin-bottom: 1.5rem;
}}
.inst-badge {{
  background: rgba(255,255,255,0.12);
  border: 1px solid rgba(255,255,255,0.2);
  border-radius: 6px; padding: 0.4rem 0.9rem;
  font-size: 0.78rem; font-weight: 600;
  color: rgba(255,255,255,0.9);
}}
.hero-stats {{
  display: grid; grid-template-columns: repeat(2,1fr); gap: 1rem;
}}
.hero-stat {{
  background: rgba(255,255,255,0.1);
  border: 1px solid rgba(255,255,255,0.15);
  border-radius: 10px; padding: 1rem 1.2rem;
}}
.hero-stat-val {{
  font-family: 'Source Serif 4', serif;
  font-size: 1.8rem; font-weight: 700;
  color: var(--amber2); line-height: 1;
}}
.hero-stat-lbl {{
  font-size: 0.75rem; color: rgba(255,255,255,0.6);
  margin-top: 0.3rem;
}}
 
/* ── PIPELINE FLOW ───────────────────────────────── */
.pipeline-flow {{
  display: flex; flex-direction: column; gap: 0.4rem;
}}
.pl-layer {{
  background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.15);
  border-radius: 8px; padding: 0.9rem 1.2rem;
  display: flex; align-items: center; justify-content: space-between;
  transition: background 0.2s;
}}
.pl-layer:hover {{ background: rgba(255,255,255,0.14); }}
.pl-name {{
  font-weight: 600; font-size: 0.85rem;
  display: flex; align-items: center; gap: 0.5rem;
}}
.pl-dot {{
  width: 10px; height: 10px; border-radius: 50%;
}}
.pl-dot.bronze {{ background: #cd9b5a; }}
.pl-dot.silver {{ background: #c0c0c0; }}
.pl-dot.gold   {{ background: var(--amber2); }}
.pl-count {{ font-size: 0.78rem; color: rgba(255,255,255,0.6); text-align:right; }}
.pl-count strong {{ display:block; color: white; font-size:0.9rem; }}
.pl-arrow {{ text-align:center; color:rgba(255,255,255,0.3); font-size:1rem; margin:-0.1rem 0; }}
 
/* ── LAYOUT ──────────────────────────────────────── */
.page {{ max-width: 1200px; margin: 0 auto; }}
 
section {{
  padding: 3rem 2rem;
  border-top: 1px solid var(--border);
}}
section:first-of-type {{ border-top: none; }}
 
.section-eyebrow {{
  font-size: 0.7rem; font-weight: 600; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--green);
  margin-bottom: 0.4rem;
}}
.section-title {{
  font-family: 'Source Serif 4', serif;
  font-size: 1.5rem; font-weight: 700;
  color: var(--brown); margin-bottom: 0.4rem;
}}
.section-desc {{
  color: var(--muted); font-size: 0.87rem;
  max-width: 680px; margin-bottom: 2rem; line-height: 1.7;
}}
 
.grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:1.5rem; }}
.grid-3 {{ display:grid; grid-template-columns:repeat(3,1fr); gap:1.5rem; }}
.grid-4 {{ display:grid; grid-template-columns:repeat(4,1fr); gap:1rem; }}
 
/* ── CARDS ───────────────────────────────────────── */
.card {{
  background: var(--white);
  border: 1px solid var(--border);
  border-radius: 12px; padding: 1.5rem;
  box-shadow: var(--shadow);
}}
.card-title {{
  font-size: 0.75rem; font-weight: 600;
  color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.06em; margin-bottom: 1rem;
}}
 
/* ── STAT CARDS ──────────────────────────────────── */
.stat-card {{
  background: var(--white);
  border: 1px solid var(--border);
  border-radius: 10px; padding: 1.2rem 1.4rem;
  box-shadow: var(--shadow);
}}
.stat-label {{ font-size: 0.73rem; color: var(--muted); font-weight:500; }}
.stat-value {{
  font-family: 'Source Serif 4', serif;
  font-size: 1.9rem; font-weight: 700;
  color: var(--brown); line-height: 1.15;
  margin-top: 0.2rem;
}}
.stat-value.green {{ color: var(--green); }}
.stat-value.amber {{ color: var(--amber); }}
.stat-value.red   {{ color: var(--red); }}
.stat-sub {{ font-size: 0.72rem; color: var(--muted2); margin-top: 0.2rem; }}
 
/* ── CONTROLS ────────────────────────────────────── */
.controls {{
  display: flex; gap: 0.75rem; flex-wrap: wrap;
  margin-bottom: 1.25rem; align-items: center;
}}
.controls label {{
  font-size: 0.78rem; font-weight: 500; color: var(--muted);
}}
select, .btn-group button {{
  font-family: 'Inter', sans-serif;
  font-size: 0.8rem; font-weight: 500;
  border: 1px solid var(--border);
  border-radius: 6px; padding: 0.4rem 0.8rem;
  background: var(--white); color: var(--brown);
  cursor: pointer; transition: all 0.15s;
}}
select:hover, .btn-group button:hover {{
  border-color: var(--brown2); background: var(--warm);
}}
.btn-group {{ display: flex; gap: 0; border-radius: 6px; overflow: hidden; }}
.btn-group button {{
  border-radius: 0; border-right-width: 0;
}}
.btn-group button:first-child {{ border-radius: 6px 0 0 6px; }}
.btn-group button:last-child  {{ border-radius: 0 6px 6px 0; border-right-width:1px; }}
.btn-group button.active {{
  background: var(--brown); color: white;
  border-color: var(--brown);
}}
 
/* ── TABS ────────────────────────────────────────── */
.tabs {{
  display: flex; gap: 0; border-bottom: 2px solid var(--border);
  margin-bottom: 1.5rem;
}}
.tab {{
  padding: 0.6rem 1.2rem; font-size: 0.82rem; font-weight: 500;
  color: var(--muted); cursor: pointer; border: none;
  background: none; border-bottom: 2px solid transparent;
  margin-bottom: -2px; transition: all 0.15s;
}}
.tab:hover {{ color: var(--brown); }}
.tab.active {{ color: var(--brown); border-bottom-color: var(--green); font-weight: 600; }}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}
 
/* ── TABLE ───────────────────────────────────────── */
.data-table {{ width:100%; border-collapse:collapse; font-size:0.82rem; }}
.data-table th {{
  text-align:left; padding:0.6rem 1rem;
  background: var(--warm); color: var(--muted);
  font-weight:600; font-size:0.72rem;
  border-bottom: 1px solid var(--border);
  text-transform:uppercase; letter-spacing:0.05em;
}}
.data-table td {{
  padding: 0.65rem 1rem;
  border-bottom: 1px solid rgba(212,197,176,0.4);
  color: var(--text);
}}
.data-table tr:last-child td {{ border-bottom:none; }}
.data-table tr:hover td {{ background: var(--warm); }}
 
/* ── BADGES ──────────────────────────────────────── */
.badge {{
  display:inline-block; padding:0.18rem 0.55rem;
  border-radius:4px; font-size:0.7rem; font-weight:600;
}}
.badge-green  {{ background:rgba(58,107,74,0.12);  color:var(--green);  }}
.badge-amber  {{ background:rgba(193,127,42,0.12); color:var(--amber);  }}
.badge-red    {{ background:rgba(176,58,46,0.12);  color:var(--red);    }}
.badge-brown  {{ background:rgba(92,74,53,0.12);   color:var(--brown);  }}
 
/* ── FINDING CARDS ───────────────────────────────── */
.finding {{
  border-left: 3px solid var(--green);
  padding: 0.9rem 1.2rem;
  background: rgba(58,107,74,0.05);
  border-radius: 0 8px 8px 0;
  margin-bottom: 0.85rem;
}}
.finding-num {{
  font-size: 0.68rem; font-weight: 700; letter-spacing:0.1em;
  text-transform: uppercase; color: var(--green);
  margin-bottom: 0.2rem;
}}
.finding-title {{ font-weight: 600; color: var(--brown); font-size:0.88rem; }}
.finding-desc  {{ color: var(--muted); font-size:0.82rem; margin-top:0.25rem; line-height:1.6; }}
 
/* ── PAPER CARDS ─────────────────────────────────── */
.paper-card {{
  background: var(--warm);
  border: 1px solid var(--border);
  border-radius: 8px; padding: 1rem 1.2rem;
  margin-bottom: 0.75rem;
}}
.paper-title  {{ font-weight:600; font-size:0.85rem; color:var(--brown); line-height:1.4; }}
.paper-meta   {{ font-size:0.75rem; color:var(--muted); margin-top:0.2rem; font-style:italic; }}
.paper-finding {{ font-size:0.78rem; color:var(--green); margin-top:0.4rem; font-weight:500; }}
 
/* ── CLEANING STEPS ──────────────────────────────── */
.step {{ display:flex; gap:1rem; margin-bottom:1.2rem; }}
.step-num {{
  min-width:28px; height:28px;
  background:var(--green); color:white;
  border-radius:50%; display:flex;
  align-items:center; justify-content:center;
  font-weight:700; font-size:0.8rem; flex-shrink:0;
  margin-top:0.1rem;
}}
.step-title {{ font-weight:600; font-size:0.85rem; color:var(--brown); }}
.step-desc  {{ font-size:0.8rem; color:var(--muted); margin-top:0.15rem; line-height:1.55; }}
 
/* ── PROFILE BARS ────────────────────────────────── */
.null-bar {{ height:5px; background:var(--warm2); border-radius:3px; overflow:hidden; margin-top:3px; }}
.null-fill {{ height:100%; border-radius:3px; transition: width 0.6s ease; }}
.null-fill.low  {{ background: var(--green2); }}
.null-fill.mid  {{ background: var(--amber);  }}
.null-fill.high {{ background: var(--red);    }}
 
/* ── VALIDATED BANNER ────────────────────────────── */
.validated-banner {{
  background: rgba(58,107,74,0.08);
  border: 1px solid rgba(58,107,74,0.25);
  border-radius: 8px; padding: 0.75rem 1rem;
  display: flex; align-items: center; gap: 0.6rem;
  font-size: 0.82rem; color: var(--green);
  font-weight: 500; margin-bottom: 1rem;
}}
 
/* ── FOOTER ──────────────────────────────────────── */
footer {{
  background: var(--brown);
  color: rgba(255,255,255,0.7);
  padding: 2.5rem 2rem; text-align: center;
  font-size: 0.8rem; line-height: 1.7;
}}
footer strong {{ color: white; }}

/* ── DATA & CODE ─────────────────────────────────── */
.access-card h3 {{ font-family:'Source Serif 4',serif; font-size:1.05rem; color:var(--brown); margin-bottom:0.4rem; }}
.access-card p  {{ font-size:0.84rem; color:var(--muted); line-height:1.6; margin-bottom:0.9rem; }}
.access-card a.btn {{ display:inline-block; font-size:0.8rem; font-weight:600; color:var(--white); background:var(--green);
  border-radius:6px; padding:0.45rem 0.9rem; text-decoration:none; margin:0 0.4rem 0.4rem 0; }}
.access-card a.btn.alt {{ background:transparent; color:var(--green); border:1px solid var(--green); }}
.access-card .pending {{ font-size:0.78rem; color:var(--amber); font-weight:600; }}
.licence-row {{ margin-top:1.5rem; font-size:0.84rem; color:var(--muted); line-height:1.7; }}
.licence-row a {{ color:var(--green); }}
nav a {{ white-space: nowrap; }}
@media (max-width: 1280px) {{ .nav-badge {{ display: none; }} }}
 
/* ── RESPONSIVE ──────────────────────────────────── */
@media (max-width: 768px) {{
  .hero-inner {{ grid-template-columns: 1fr; gap: 2rem; }}
  .grid-2, .grid-3, .grid-4 {{ grid-template-columns: 1fr; }}
  .controls, .btn-group {{ flex-wrap: wrap; max-width: 100%; }}
  section {{ padding: 2.5rem 1rem; }}
  section * {{ min-width: 0; }}
  .paper-card {{ overflow-wrap: anywhere; }}
  nav a {{ display: none; }}
}}
</style>
</head>
<body>
 
<!-- NAV -->
<nav>
  <div class="nav-logo">Sense<span>Of</span>Sensors</div>
  <a href="#access">Data &amp; Code</a>
  <a href="#overview">Overview</a>
  <a href="#behaviour">Behaviour</a>
  <a href="#clinical">Clinical</a>
  <a href="#heat">Heat Stress</a>
  <a href="#repro">Reproduction</a>
{milk_nav}  <a href="#research">Research</a>
  <a href="#pipeline">Pipeline</a>
  <a href="#profile">Profile</a>
  <div class="nav-spacer"></div>
  <span class="nav-badge">8 Farms · 2016–2020</span>
</nav>
 
<!-- HERO -->
<div class="hero">
  <div class="hero-inner page">
    <div>
      <div class="hero-eyebrow">Precision Dairy Farming Analytics</div>
      <h1 class="hero-title">
        Leg &amp; Neck Sensor Data<br>from <em>Commercial Dairy Farms</em><br>— Research Report
      </h1>
      <p class="hero-desc">
        A complete medallion data pipeline processing 1.2 billion leg- and neck-sensor readings
        from 8 commercial dairy farms{milk_hero}. Covering behaviour, heat stress, transition
        cow health{milk_topic}, and reproductive performance across 2016–2020.
      </p>
      <div class="hero-institutions">
        <div class="inst-badge">Utrecht University</div>
        <div class="inst-badge">Cornell University</div>
      </div>
      <div class="hero-stats">
        <div class="hero-stat">
          <div class="hero-stat-val">1.2B</div>
          <div class="hero-stat-lbl">Raw sensor readings</div>
        </div>
        <div class="hero-stat">
          <div class="hero-stat-val">8</div>
          <div class="hero-stat-lbl">Commercial dairy farms</div>
        </div>
        <div class="hero-stat">
          <div class="hero-stat-val">1,648</div>
          <div class="hero-stat-lbl">Cow-lactation records</div>
        </div>
        <div class="hero-stat">
          <div class="hero-stat-val">158</div>
          <div class="hero-stat-lbl">Analysis columns in GoldMaster</div>
        </div>
      </div>
    </div>
 
    <div class="pipeline-flow">
      <div style="font-size:0.72rem;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;color:rgba(255,255,255,0.5);margin-bottom:0.5rem">
        Medallion Pipeline
      </div>
      <div class="pl-layer">
        <div class="pl-name"><div class="pl-dot bronze"></div>Bronze — Raw Ingestion</div>
        <div class="pl-count"><strong>1,214,421,979</strong>SensorData rows</div>
      </div>
      <div class="pl-arrow">↓</div>
      <div class="pl-layer">
        <div class="pl-name"><div class="pl-dot bronze"></div>Bronze — Calendar Events</div>
        <div class="pl-count"><strong>85,578</strong>raw events</div>
      </div>
      <div class="pl-arrow">↓</div>
      <div class="pl-layer">
        <div class="pl-name"><div class="pl-dot silver"></div>Silver — {n_datasets} Datasets Cleaned</div>
        <div class="pl-count"><strong>5 metadata tables</strong>rules-as-data</div>
      </div>
      <div class="pl-arrow">↓</div>
      <div class="pl-layer">
        <div class="pl-name"><div class="pl-dot gold"></div>Gold — {gold_tables} Delta Tables</div>
        <div class="pl-count"><strong>2,530,267</strong>GoldMaster rows</div>
      </div>
    </div>
  </div>
</div>
 
<div class="page">
{access_section}
<!-- OVERVIEW -->
<section id="overview">
  <div class="section-eyebrow">Dataset Overview</div>
  <div class="section-title">Eight Farms, Five Years, Five Behaviours</div>
  <p class="section-desc">
    Each cow wore a leg sensor and a neck sensor, which together record lying, standing,
    walking, eating, ruminating and inactive time. One reading every 15 minutes per cow. Data collected across 8 commercial
    dairy herds spanning a range of herd sizes, management systems, and housing types.
  </p>
 
  <div class="grid-4" style="margin-bottom:1.5rem">
    <div class="stat-card">
      <div class="stat-label">Distinct animals monitored</div>
      <div class="stat-value green">1,470</div>
      <div class="stat-sub">Across all 8 farms</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total animal-days</div>
      <div class="stat-value">2.53M</div>
      <div class="stat-sub">One row per animal per day</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Clinical records</div>
      <div class="stat-value amber">1,648</div>
      <div class="stat-sub">Cow-lactation calving records</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Calendar events</div>
      <div class="stat-value">48,814</div>
      <div class="stat-sub">Calving, dryoff, insemination</div>
    </div>
  </div>
 
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Animal-Days per Farm</div>
      <div id="farmDaysCtrl" class="controls">
        <label>Sort by:</label>
        <div class="btn-group">
          <button class="active" onclick="sortFarmDays('farm')">Farm ID</button>
          <button onclick="sortFarmDays('days')">Days</button>
          <button onclick="sortFarmDays('animals')">Animals</button>
        </div>
      </div>
      <div style="position:relative;height:220px">
        <canvas id="farmDaysChart"></canvas>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Identity Bridge — Sensor to Clinical Match</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:0.75rem">
        How sensor animals were matched to ReferenceData clinical records.
        68.3% of sensor animals matched. 31.7% are herd animals outside the study cohort.
      </p>
      <div style="position:relative;height:200px">
        <canvas id="bridgeChart"></canvas>
      </div>
    </div>
  </div>
</section>
 
<!-- BEHAVIOUR -->
<section id="behaviour">
  <div class="section-eyebrow">Behavioural Time Budgets</div>
  <div class="section-title">Daily Activity Patterns</div>
  <p class="section-desc">
    Each 15-minute window records minutes spent in each behaviour (0–15 min per window).
    Summed to daily totals. Null means no sensor reading for that animal-day — not zero activity.
  </p>
 
  <!-- Tabs for behaviour views -->
  <div class="tabs">
    <button class="tab active" onclick="showBehTab('dim')">By Days In Milk</button>
    <button class="tab" onclick="showBehTab('parity')">By Parity</button>
    <button class="tab" onclick="showBehTab('farm')">By Farm</button>
    <button class="tab" onclick="showBehTab('precalv')">Pre-Calving</button>
  </div>
 
  <div id="beh-dim" class="tab-panel active">
    <div class="controls">
      <label>Behaviour:</label>
      <div class="btn-group" id="dimBehBtns">
        <button class="active" onclick="updateDimChart('Eating')">Eating</button>
        <button onclick="updateDimChart('Lying')">Lying</button>
        <button onclick="updateDimChart('Ruminating')">Ruminating</button>
        <button onclick="updateDimChart('Walking')">Walking</button>
        <button onclick="updateDimChart('Standing')">Standing</button>
      </div>
      <label style="margin-left:1rem">Farm:</label>
      <select id="dimFarmSelect" onchange="updateDimChart()">
        <option value="all">All farms</option>
      </select>
    </div>
    <div class="card">
      <div class="card-title" id="dimChartTitle">Average Eating Time — First 100 Days In Milk (all farms)</div>
      <div style="position:relative;height:300px">
        <canvas id="dimTrendChart"></canvas>
      </div>
    </div>
  </div>
 
  <div id="beh-parity" class="tab-panel">
    <div class="card">
      <div class="card-title">Behaviour by Parity — Average Daily Minutes</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:1rem">
        First-calving cows (Parity 1) show distinctly different patterns from multiparous cows.
        Click a behaviour to highlight it.
      </p>
      <div style="position:relative;height:300px">
        <canvas id="parityChart"></canvas>
      </div>
    </div>
  </div>
 
  <div id="beh-farm" class="tab-panel">
    <div class="card">
      <div class="card-title">Average Daily Behaviour per Farm</div>
      <div style="position:relative;height:300px">
        <canvas id="farmBehChart"></canvas>
      </div>
    </div>
  </div>
 
  <div id="beh-precalv" class="tab-panel">
    <div class="validated-banner">
      ✓ Validated against van Leerdam et al. (2022) — all four key findings replicated
    </div>
    <div class="controls">
      <label>Behaviour:</label>
      <div class="btn-group">
        <button class="active" onclick="updatePrecalvChart('Eating')">Eating</button>
        <button onclick="updatePrecalvChart('Standing')">Standing</button>
        <button onclick="updatePrecalvChart('Lying')">Lying</button>
        <button onclick="updatePrecalvChart('Walking')">Walking</button>
        <button onclick="updatePrecalvChart('Ruminating')">Ruminating</button>
      </div>
    </div>
    <div class="grid-2">
      <div class="card">
        <div class="card-title" id="precalvTitle">Eating — 21 Days Before Calving by Calcium Status</div>
        <div style="position:relative;height:260px">
          <canvas id="precalvChart"></canvas>
        </div>
      </div>
      <div class="card">
        <div class="card-title">Validated Findings vs van Leerdam et al.</div>
        <div class="finding">
          <div class="finding-num">Finding 1 ✓</div>
          <div class="finding-title">Normocalcaemic cows eat more overall</div>
          <div class="finding-desc">14.4 min/day more eating. Difference increases from 11.2 to 17.9 min/day closer to calving.</div>
        </div>
        <div class="finding">
          <div class="finding-num">Finding 2 ✓</div>
          <div class="finding-title">Normocalcaemic cows walk more</div>
          <div class="finding-desc">3.1 min/day more walking throughout the pre-calving period.</div>
        </div>
        <div class="finding">
          <div class="finding-num">Finding 3 ✓</div>
          <div class="finding-title">Hypocalcaemic cows stand more in last 5 days</div>
          <div class="finding-desc">Hypo: 643.5 vs Normo: 630.7 min/day in DIM -5 to -1.</div>
        </div>
        <div class="finding">
          <div class="finding-num">Finding 4 ✓</div>
          <div class="finding-title">Eating gap widens approaching calving</div>
          <div class="finding-desc">DIM -21 to -11: 11.2 min gap. DIM -10 to -1: 17.9 min gap.</div>
        </div>
      </div>
    </div>
  </div>
</section>
 
<!-- CLINICAL -->
<section id="clinical">
  <div class="section-eyebrow">Transition Cow Health</div>
  <div class="section-title">Clinical Measurements at Calving</div>
  <p class="section-desc">
    Blood calcium (Ca), β-hydroxybutyric acid (BHBA), body condition score (BCS), and
    locomotion score collected within 48 hours of calving by a veterinarian.
    1,648 cow-lactation records across all 8 farms.
  </p>
 
  <div class="grid-4" style="margin-bottom:1.5rem">
    <div class="stat-card">
      <div class="stat-label">Subclinical Ketosis rate</div>
      <div class="stat-value amber">34.9%</div>
      <div class="stat-sub">BHBA ≥ 1.2 mmol/L at either sample</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Subclinical Hypocalcaemia</div>
      <div class="stat-value amber">10.6%</div>
      <div class="stat-sub">Blood Ca &lt; 2.0 mmol/L</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Difficult Calving</div>
      <div class="stat-value red">10.9%</div>
      <div class="stat-sub">CalvingType 3 (Difficult) or 4 (Caesarean)</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Retained Placenta</div>
      <div class="stat-value red">16.3%</div>
      <div class="stat-sub">Afterbirth retained &gt; 8 hours</div>
    </div>
  </div>
 
  <div class="grid-2" style="margin-bottom:1.5rem">
    <div class="card">
      <div class="card-title">Clinical Condition Rates per Farm</div>
      <div class="controls">
        <label>Condition:</label>
        <div class="btn-group">
          <button class="active" onclick="updateClinicalChart('Ketosis')">Ketosis</button>
          <button onclick="updateClinicalChart('Hypocalcaemia')">Hypocalcaemia</button>
          <button onclick="updateClinicalChart('Difficult')">Difficult Calving</button>
          <button onclick="updateClinicalChart('Retained')">Retained Placenta</button>
        </div>
      </div>
      <div style="position:relative;height:220px">
        <canvas id="clinicalChart"></canvas>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Top Clinical Conditions from Farm Notes</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:0.75rem">
        TranslationGlossary applied to free-text farm notes. 94 terms mapped to 61 boolean flags.
      </p>
      <div style="position:relative;height:240px">
        <canvas id="flagChart"></canvas>
      </div>
    </div>
  </div>
</section>
 
<!-- HEAT STRESS -->
<section id="heat">
  <div class="section-eyebrow">Temperature-Humidity Index</div>
  <div class="section-title">Heat Stress Exposure</div>
  <p class="section-desc">
    Hourly THI from indoor and outdoor climate sensors. Heat stress threshold: THI &gt; 72.
    Severe: THI &gt; 80. Research shows behavioural changes from THI 56 — well below the
    traditionally accepted threshold.
  </p>
 
  <div class="grid-4" style="margin-bottom:1.5rem">
    <div class="stat-card">
      <div class="stat-label">Maximum THI recorded</div>
      <div class="stat-value red">84.4</div>
      <div class="stat-sub">Farm G — Severe heat stress</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Peak heat stress days</div>
      <div class="stat-value amber">157</div>
      <div class="stat-sub">Farm F — highest exposure</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Severe stress days (all farms)</div>
      <div class="stat-value red">62</div>
      <div class="stat-sub">THI &gt; 80 across all farms 2017–2020</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Internal sensor coverage</div>
      <div class="stat-value green">48%</div>
      <div class="stat-sub">Farms with indoor THI sensors</div>
    </div>
  </div>
 
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Mean THI by Month — All Farms Combined</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:0.75rem">
        Dashed line shows heat stress threshold (THI 72). Summer months (Jun–Aug)
        consistently exceed the threshold.
      </p>
      <div style="position:relative;height:250px">
        <canvas id="thiMonthChart"></canvas>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Heat Stress Days per Farm (THI &gt; 72)</div>
      <div style="position:relative;height:250px">
        <canvas id="thiStressChart"></canvas>
      </div>
    </div>
  </div>
</section>
 
{milk_section}
<!-- REPRODUCTION -->
<section id="repro">
  <div class="section-eyebrow">Reproductive Performance</div>
  <div class="section-title">Calving Intervals & Insemination Patterns</div>
  <p class="section-desc">
    Derived from CalendarData: insemination events, heat detection, pregnancy checks,
    dryoff, and next calving dates. 1,570 of 1,648 lactations have reproductive event data.
  </p>
 
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Reproductive Performance per Farm</div>
      <div class="controls">
        <label>Metric:</label>
        <div class="btn-group">
          <button class="active" onclick="updateReproChart('DaysOpen')">Days Open</button>
          <button onclick="updateReproChart('Interval')">Calving Interval</button>
          <button onclick="updateReproChart('Inseminations')">Avg Inseminations</button>
        </div>
      </div>
      <div style="position:relative;height:240px">
        <canvas id="reproChart"></canvas>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Inseminations per Lactation — Distribution</div>
      <p style="font-size:0.78rem;color:var(--muted);margin-bottom:0.75rem">
        578 cows (37%) conceived at first insemination. 13 required 10+ inseminations.
        Target: conception within 2–3 inseminations.
      </p>
      <div style="position:relative;height:240px">
        <canvas id="insemChart"></canvas>
      </div>
    </div>
  </div>
</section>
 
<!-- RESEARCH -->
<section id="research">
  <div class="section-eyebrow">Research Applications</div>
  <div class="section-title">Published Findings & Future Directions</div>
  <p class="section-desc">
    GoldMaster provides a single 158-column analysis-ready table. Any research question
    combining behaviour, clinical outcomes, heat stress, and reproductive performance
    can be answered directly without additional joins.
  </p>
 
  <div class="grid-2">
    <div>
      <div style="font-size:0.75rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:1rem">Published Papers Using This Dataset</div>
      <div class="paper-card">
        <div class="paper-title">Sensor based time budgets in commercial Dutch dairy herds vary over lactation cycles and within 24 hours</div>
        <div class="paper-meta">Hut, Kuiper, Nielen, Hulsen, Stassen &amp; Hostens — PLOS ONE, 2022 · doi:10.1371/journal.pone.0264392</div>
        <div class="paper-finding">Time budgets (eating, ruminating, lying, standing, walking) from leg and neck sensors across lactation and the day</div>
      </div>
      <div class="paper-card">
        <div class="paper-title">Heat stress in a temperate climate leads to adapted sensor-based behavioral patterns of dairy cows</div>
        <div class="paper-meta">Hut, Scheurwater, Nielen, van den Broek &amp; Hostens — Journal of Dairy Science 105(8), 2022 · doi:10.3168/jds.2021-21756</div>
        <div class="paper-finding">Behavioural responses to heat stress in a temperate climate</div>
      </div>
      <div class="paper-card">
        <div class="paper-title">Sensor based eating time variables of dairy cows in the transition period related to the time to first service</div>
        <div class="paper-meta">Hut et al. — Preventive Veterinary Medicine 169, 2019 · doi:10.1016/j.prevetmed.2019.104694</div>
        <div class="paper-finding">Eating time around calving and its relation to the time to first service</div>
      </div>
      <div class="paper-card">
        <div class="paper-title">A predictive model for hypocalcaemia in dairy cows utilizing behavioural sensor data</div>
        <div class="paper-meta">van Leerdam et al. — Computers and Electronics in Agriculture, 2024</div>
        <div class="paper-finding">Predicting (sub)clinical hypocalcaemia from sensor-based behaviour</div>
      </div>
 
      <div style="font-size:0.75rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin:1.5rem 0 0.75rem">Future Research Directions</div>
      <div style="display:flex;flex-wrap:wrap;gap:0.4rem">
        <span class="badge badge-green">Ketosis early detection</span>
        <span class="badge badge-green">Lameness prediction</span>
        <span class="badge badge-green">Heat stress × reproduction</span>
        <span class="badge badge-amber">Multi-farm transfer learning</span>
        <span class="badge badge-amber">Parity-specific behaviour models</span>
        <span class="badge badge-amber">BCS proxy from lying time</span>
        <span class="badge badge-brown">Real-time alerting thresholds</span>
        <span class="badge badge-brown">Sensor reassignment detection</span>
        <span class="badge badge-brown">Pre-calving risk scoring</span>
      </div>
    </div>
 
    <div>
      <div style="font-size:0.75rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:1rem">GoldMaster Column Groups</div>
      <div class="card" style="padding:1rem">
        <table class="data-table">
          <thead><tr><th>Group</th><th>Columns</th><th>Description</th></tr></thead>
          <tbody>
            <tr><td>Identity</td><td><span class="badge badge-brown">6</span></td><td>Farm, animal, join keys</td></tr>
            <tr><td>Time</td><td><span class="badge badge-brown">8</span></td><td>DIM, phase, season, parity</td></tr>
            <tr><td>Behaviour 15-min</td><td><span class="badge badge-green">9</span></td><td>Eating, lying, walking, standing, ruminating</td></tr>
            <tr><td>Behaviour 2-hr</td><td><span class="badge badge-green">4</span></td><td>2-hour window summaries</td></tr>
            <tr><td>Behaviour daily</td><td><span class="badge badge-green">16</span></td><td>Bout statistics, daily summaries</td></tr>
            <tr><td>THI</td><td><span class="badge badge-amber">12</span></td><td>Heat stress metrics, internal/external</td></tr>
            <tr><td>Clinical</td><td><span class="badge badge-amber">17</span></td><td>Calcium, BHBA, BCS, calving type</td></tr>
            <tr><td>Composite flags</td><td><span class="badge badge-amber">4</span></td><td>Ketosis, hypocalcaemia, difficult calving</td></tr>
            <tr><td>Reproductive</td><td><span class="badge badge-amber">23</span></td><td>Insemination, pregnancy, calving interval</td></tr>
            <tr><td>Note flags</td><td><span class="badge badge-brown">61</span></td><td>Boolean flags from farm notes</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</section>
 
<!-- PIPELINE -->
<section id="pipeline">
  <div class="section-eyebrow">Data Engineering</div>
  <div class="section-title">Bronze → Silver → Gold Cleaning Pipeline</div>
  <p class="section-desc">
    All cleaning logic declared in 5 shared Delta metadata tables — never hardcoded in notebooks.
    Silver keeps all rows; problems become Warnings array annotations, never silent drops.
  </p>
 
  <div class="grid-3" style="margin-bottom:1.5rem">
    <div class="card">
      <div class="card-title" style="color:#9a7040">⬡ Bronze Layer — Raw Ingestion</div>
      <div class="step">
        <div class="step-num" style="background:#9a7040">1</div>
        <div><div class="step-title">{n_datasets} source datasets</div>
        <div class="step-desc">ReferenceData (Parquet), SensorData (Parquet, 23 folders), THI (CSV), CalendarData (CSV, 2 batches){milk_bronze}. Read-only — never modified.</div></div>
      </div>
      <div class="step">
        <div class="step-num" style="background:#9a7040">2</div>
        <div><div class="step-title">Calendar data recovery</div>
        <div class="step-desc">DryoffEvent and CalvingEvent CSVs excluded from original ingestion — re-ingested from blob storage. Provides actual lactation boundary dates.</div></div>
      </div>
      <div class="step">
        <div class="step-num" style="background:#9a7040">3</div>
        <div><div class="step-title">63 positional columns mapped</div>
        <div class="step-desc">ReferenceData had no column headers — all 63 columns identified by position and named. ObservationType folder structure documented.</div></div>
      </div>
    </div>
 
    <div class="card">
      <div class="card-title" style="color:#888">⬡ Silver Layer — Cleaning</div>
      <div class="step">
        <div class="step-num">1</div>
        <div><div class="step-title">PascalCase normalisation</div>
        <div class="step-desc">All column names, ObservationType folder names, and metadata fields normalised to PascalCase. Source was camelCase with mixed underscores.</div></div>
      </div>
      <div class="step">
        <div class="step-num">2</div>
        <div><div class="step-title">Type casting & range validation</div>
        <div class="step-desc">Dates → date/timestamp. Numerics → double. BCS capped at 5. Values flagged with Warnings array — never silently dropped.</div></div>
      </div>
      <div class="step">
        <div class="step-num">3</div>
        <div><div class="step-title">Deduplication</div>
        <div class="step-desc">SensorData: 9.5M exact duplicates removed. Calendar batches: 36.7K overlap duplicates removed. Corrupt dates (year &lt; 2000) dropped.</div></div>
      </div>
      <div class="step">
        <div class="step-num">4</div>
        <div><div class="step-title">Farm notes → English glossary</div>
        <div class="step-desc">94 terms in Notes and IndividualAttention columns mapped to English. Applied in Gold as 61 boolean condition flags.</div></div>
      </div>
    </div>
 
    <div class="card">
      <div class="card-title" style="color:#c17f2a">⬡ Gold Layer — Enrichment</div>
      <div class="step">
        <div class="step-num" style="background:var(--amber)">1</div>
        <div><div class="step-title">3-stddev outlier filter</div>
        <div class="step-desc">Applied per ObservationType before daily aggregation. Translated from original Scala research notebooks. Removes statistically extreme readings.</div></div>
      </div>
      <div class="step">
        <div class="step-num" style="background:var(--amber)">2</div>
        <div><div class="step-title">1.2B → 2.5M daily pivot</div>
        <div class="step-desc">Raw sensor readings aggregated to one row per animal per day. Values already in minutes. Impossible daily totals (&gt; 1440 min) nulled.</div></div>
      </div>
      <div class="step">
        <div class="step-num" style="background:var(--amber)">3</div>
        <div><div class="step-title">Lactation window construction</div>
        <div class="step-desc">DryoffEvent dates replace estimated LactationEnd for 801 of 1,648 windows. Extended 21 days pre-calving for transition cow analysis.</div></div>
      </div>
      <div class="step">
        <div class="step-num" style="background:var(--amber)">4</div>
        <div><div class="step-title">158-column GoldMaster join</div>
        <div class="step-desc">All 7 gold tables joined via left joins — no data dropped. DaysInMilk, LactationPhase, CalvingSeason, PreCalvingWeek derived columns added.</div></div>
      </div>
    </div>
  </div>
 
  <div class="card">
    <div class="card-title">Rulebook — Declared Columns per Dataset</div>
    <div id="rulebook-bars" style="padding:0.5rem 0"></div>
  </div>
</section>
 
<!-- PROFILE -->
<section id="profile">
  <div class="section-eyebrow">Data Quality</div>
  <div class="section-title">GoldMaster Profile Report</div>
  <p class="section-desc">
    Null rates for key GoldMaster columns across 2,530,267 rows.
    High null rates are expected — clinical columns only populate within matched lactation windows,
    which cover 21.2% of total animal-days.
  </p>
 
  <div class="grid-2">
    <div class="card">
      <div class="card-title">Null Rate by Column</div>
      <div id="null-profile-bars" style="padding:0.25rem 0"></div>
    </div>
    <div class="card">
      <div class="card-title">Coverage Explanation</div>
      <table class="data-table">
        <thead><tr><th>Column</th><th>Null %</th><th>Expected Reason</th></tr></thead>
        <tbody>
          <tr><td>DailyEatingMin15</td><td><span class="badge badge-amber">~39%</span></td><td>Animals without 15-min sensor type</td></tr>
          <tr><td>MaxTHIExternal</td><td><span class="badge badge-amber">~15%</span></td><td>2016 sensor days precede THI coverage (starts 2017)</td></tr>
          <tr><td>SubclinicalKetosis</td><td><span class="badge badge-red">~82%</span></td><td>Only within matched lactation windows (21.2%)</td></tr>
          <tr><td>CalciumLevel</td><td><span class="badge badge-red">~82%</span></td><td>Only within matched lactation windows</td></tr>
          <tr><td>InseminationCount</td><td><span class="badge badge-red">~80%</span></td><td>Only within matched lactation windows</td></tr>
          <tr><td>LactationStart</td><td><span class="badge badge-amber">~79%</span></td><td>Sensor days outside lactation window boundaries</td></tr>
          <tr><td>Parity</td><td><span class="badge badge-amber">~79%</span></td><td>Sensor days outside lactation window boundaries</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</section>
 
</div><!-- end .page -->
 
<footer>
  <strong>SenseOfSensors</strong> — Precision Dairy Farming Analytics Pipeline<br>
  Utrecht University · Cornell University<br>
  Dataset collected 2016–2021 · Pipeline rebuilt 2026 · 8 commercial dairy farms
</footer>
 
<script>
const D = {data_json};
const farmName = h => String(h).replace(/^Farm_/, 'Farm ');
 
// ── Colour palette ────────────────────────────────
const C = {{
  green:  '#3a6b4a', green2: '#5a9468', green3: '#c8dfc8',
  amber:  '#c17f2a', amber2: '#e8a84a', amber3: '#fdf0d5',
  red:    '#b03a2e', red2:   '#e8a09a',
  brown:  '#5c4a35', brown2: '#8b6f47',
  muted:  '#a89880', border: '#d4c5b0',
  farms:  ['#3a6b4a','#c17f2a','#5a9468','#8b6f47','#b03a2e','#e8a84a','#7b9e87','#9a7040'],
}};
 
Chart.defaults.font.family = 'Inter, sans-serif';
Chart.defaults.font.size   = 11;
Chart.defaults.color       = '#7a6a57';
Chart.defaults.borderColor = '#d4c5b0';
 
const chartDefaults = {{
  responsive: true,
  maintainAspectRatio: false,
  plugins: {{
    legend: {{ labels: {{ color: '#7a6a57', padding: 12 }} }},
    tooltip: {{ backgroundColor: '#faf7f2', titleColor: '#2d2416', bodyColor: '#7a6a57',
                borderColor: '#d4c5b0', borderWidth: 1, padding: 10 }}
  }}
}};
 
function mk(id, cfg) {{
  const el = document.getElementById(id);
  if (!el) return null;
  return new Chart(el, cfg);
}}
 
// ── Tab switching ─────────────────────────────────
function showBehTab(name) {{
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('beh-' + name).classList.add('active');
  event.target.classList.add('active');
}}
 
// ── Button group helper ───────────────────────────
function activateBtn(btn) {{
  btn.closest('.btn-group').querySelectorAll('button').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
}}
 
// ── Farm days chart ───────────────────────────────
let farmDaysChart;
let farmDaysSortMode = 'farm';
 
function sortFarmDays(mode) {{
  farmDaysSortMode = mode;
  activateBtn(event.target);
  let data = [...D.farm_stats];
  if (mode === 'days')    data.sort((a,b) => b.AnimalDays - a.AnimalDays);
  if (mode === 'animals') data.sort((a,b) => b.Animals - a.Animals);
  farmDaysChart.data.labels   = data.map(r => farmName(r.HerdIdentifier));
  farmDaysChart.data.datasets[0].data = data.map(r => r.AnimalDays);
  farmDaysChart.update('active');
}}
 
farmDaysChart = mk('farmDaysChart', {{
  type: 'bar',
  data: {{
    labels: D.farm_stats.map(r => farmName(r.HerdIdentifier)),
    datasets: [{{ label: 'Animal-Days',
      data: D.farm_stats.map(r => r.AnimalDays),
      backgroundColor: C.farms,
      borderWidth: 0, borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend: {{ display: false }} }},
    scales: {{
      y: {{ grid: {{ color: '#ede3d4' }}, ticks: {{ callback: v => (v/1000).toFixed(0)+'K' }} }},
      x: {{ grid: {{ display: false }} }}
    }}
  }}
}});
 
// ── Bridge chart ──────────────────────────────────
mk('bridgeChart', {{
  type: 'doughnut',
  data: {{
    labels: D.bridge_stats.map(r => r.JoinMethod),
    datasets: [{{ data: D.bridge_stats.map(r => r.count),
      backgroundColor: [C.green, C.amber, C.muted],
      borderColor: '#faf7f2', borderWidth: 3,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins,
      legend: {{ position: 'bottom', labels: {{ color: '#7a6a57', padding: 14 }} }},
    }}
  }}
}});
 
// ── DIM trend chart ───────────────────────────────
let dimChart;
let dimBeh = 'Eating';
 
// Populate farm select
const dimSel = document.getElementById('dimFarmSelect');
D.farm_stats.forEach(r => {{
  const o = document.createElement('option');
  o.value = r.HerdIdentifier; o.text = farmName(r.HerdIdentifier);
  dimSel.appendChild(o);
}});
 
function updateDimChart(beh) {{
  if (beh) {{ dimBeh = beh; activateBtn(event.target); }}
  const farm = dimSel.value;
  let chartData;
  if (farm === 'all') {{
    chartData = D.dim_trend;
  }} else {{
    chartData = D.farm_dim.filter(r => String(r.HerdIdentifier) === String(farm));
  }}
  const labels = chartData.map(r => r.DaysInMilk);
  const vals   = chartData.map(r => r[dimBeh] || null);
  const farmLabel = farm === 'all' ? 'all farms' : farmName(farm);
  document.getElementById('dimChartTitle').textContent =
    `Average ${{dimBeh}} Time — First 100 Days In Milk (${{farmLabel}})`;
  dimChart.data.labels = labels;
  dimChart.data.datasets[0].data = vals;
  dimChart.data.datasets[0].label = dimBeh + ' (min/day)';
  dimChart.update('active');
}}
 
dimChart = mk('dimTrendChart', {{
  type: 'line',
  data: {{
    labels: D.dim_trend.map(r => r.DaysInMilk),
    datasets: [{{ label: 'Eating (min/day)',
      data: D.dim_trend.map(r => r.Eating),
      borderColor: C.green, backgroundColor: C.green + '18',
      fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2.5,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend: {{ display: false }} }},
    scales: {{
      x: {{ title: {{ display:true, text:'Days In Milk', color: C.muted }},
             grid: {{ color: '#ede3d4' }} }},
      y: {{ title: {{ display:true, text:'Minutes / day', color: C.muted }},
             grid: {{ color: '#ede3d4' }} }}
    }}
  }}
}});
 
// ── Parity chart ──────────────────────────────────
mk('parityChart', {{
  type: 'bar',
  data: {{
    labels: D.parity_beh.map(r => 'Parity ' + r.Parity),
    datasets: [
      {{ label:'Eating',     data:D.parity_beh.map(r=>r.Eating),
         backgroundColor:C.green+'bb', borderRadius:3, borderWidth:0 }},
      {{ label:'Lying',      data:D.parity_beh.map(r=>r.Lying),
         backgroundColor:C.amber+'bb', borderRadius:3, borderWidth:0 }},
      {{ label:'Ruminating', data:D.parity_beh.map(r=>r.Ruminating),
         backgroundColor:C.brown2+'bb', borderRadius:3, borderWidth:0 }},
      {{ label:'Walking',    data:D.parity_beh.map(r=>r.Walking),
         backgroundColor:C.red+'99', borderRadius:3, borderWidth:0 }},
    ]
  }},
  options: {{ ...chartDefaults,
    scales: {{
      y: {{ grid: {{ color:'#ede3d4' }}, title:{{display:true,text:'Minutes/day',color:C.muted}} }},
      x: {{ grid: {{ display:false }} }}
    }}
  }}
}});
 
// ── Farm behaviour chart ──────────────────────────
mk('farmBehChart', {{
  type: 'bar',
  data: {{
    labels: D.farm_stats.map(r => farmName(r.HerdIdentifier)),
    datasets: [
      {{ label:'Avg Eating',     data:D.farm_stats.map(r=>r.AvgEating),
         backgroundColor:C.green+'bb', borderRadius:3, borderWidth:0 }},
      {{ label:'Avg Lying',      data:D.farm_stats.map(r=>r.AvgLying),
         backgroundColor:C.amber+'bb', borderRadius:3, borderWidth:0 }},
      {{ label:'Avg Ruminating', data:D.farm_stats.map(r=>r.AvgRuminating),
         backgroundColor:C.brown2+'bb', borderRadius:3, borderWidth:0 }},
    ]
  }},
  options: {{ ...chartDefaults,
    scales: {{
      y: {{ grid:{{color:'#ede3d4'}}, title:{{display:true,text:'Minutes/day',color:C.muted}} }},
      x: {{ grid:{{display:false}} }}
    }}
  }}
}});
 
// ── Pre-calving chart ─────────────────────────────
let precalvChart;
let precalvBeh = 'Eating';
 
function updatePrecalvChart(beh) {{
  if (beh) {{ precalvBeh = beh; activateBtn(event.target); }}
  const normo = D.precalving.filter(r => r.SubclinicalHypocalcaemia === false);
  const hypo  = D.precalving.filter(r => r.SubclinicalHypocalcaemia === true);
  document.getElementById('precalvTitle').textContent =
    `${{beh}} — 21 Days Before Calving by Calcium Status`;
  precalvChart.data.labels = normo.map(r => r.DaysInMilk);
  precalvChart.data.datasets[0].data = normo.map(r => r[beh] || null);
  precalvChart.data.datasets[1].data = hypo.map(r => r[beh] || null);
  precalvChart.update('active');
}}
 
const pcNormo = D.precalving.filter(r => r.SubclinicalHypocalcaemia === false);
const pcHypo  = D.precalving.filter(r => r.SubclinicalHypocalcaemia === true);
 
precalvChart = mk('precalvChart', {{
  type: 'line',
  data: {{
    labels: pcNormo.map(r => r.DaysInMilk),
    datasets: [
      {{ label: 'Normocalcaemic (Ca > 2.0)',
         data: pcNormo.map(r => r.Eating),
         borderColor: C.green, backgroundColor: C.green+'15',
         fill: true, tension: 0.4, pointRadius: 3, borderWidth: 2.5 }},
      {{ label: 'Hypocalcaemic (Ca ≤ 2.0)',
         data: pcHypo.map(r => r.Eating),
         borderColor: C.red, backgroundColor: C.red+'15',
         fill: true, tension: 0.4, pointRadius: 3, borderWidth: 2.5 }},
    ]
  }},
  options: {{ ...chartDefaults,
    scales: {{
      x: {{ title:{{display:true,text:'Days In Milk (negative = pre-calving)',color:C.muted}},
             grid:{{color:'#ede3d4'}}, reverse: true }},
      y: {{ title:{{display:true,text:'Minutes / day',color:C.muted}},
             grid:{{color:'#ede3d4'}} }}
    }}
  }}
}});
 
// ── Clinical chart ────────────────────────────────
let clinicalChart;
let clinicalMetric = 'Ketosis';
 
function updateClinicalChart(metric) {{
  clinicalMetric = metric;
  activateBtn(event.target);
  const keyMap = {{
    Ketosis:'KetosisRate', Hypocalcaemia:'HypocalcaemiaRate',
    Difficult:'DifficultRate', Retained:'RetainedRate'
  }};
  const colMap = {{
    Ketosis:C.amber, Hypocalcaemia:C.amber, Difficult:C.red, Retained:C.red
  }};
  const key = keyMap[metric];
  clinicalChart.data.datasets[0].data = D.clinical_rates.map(r => r[key] || 0);
  clinicalChart.data.datasets[0].backgroundColor = colMap[metric] + 'bb';
  clinicalChart.data.datasets[0].borderColor = colMap[metric];
  clinicalChart.data.datasets[0].label = metric + ' rate (%)';
  clinicalChart.update('active');
}}
 
clinicalChart = mk('clinicalChart', {{
  type: 'bar',
  data: {{
    labels: D.clinical_rates.map(r => farmName(r.HerdIdentifier)),
    datasets: [{{ label: 'Ketosis rate (%)',
      data: D.clinical_rates.map(r => r.KetosisRate || 0),
      backgroundColor: C.amber + 'bb', borderColor: C.amber,
      borderWidth: 1, borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend: {{ display: false }} }},
    scales: {{
      y: {{ grid:{{color:'#ede3d4'}}, title:{{display:true,text:'%',color:C.muted}}, max:60 }},
      x: {{ grid:{{display:false}} }}
    }}
  }}
}});
 
// ── Flag chart ────────────────────────────────────
mk('flagChart', {{
  type: 'bar',
  indexAxis: 'y',
  data: {{
    labels: D.flag_counts.map(r => r.Flag.replace('Note','')),
    datasets: [{{ label: 'Occurrences',
      data: D.flag_counts.map(r => r.Count),
      backgroundColor: C.green + 'aa',
      borderColor: C.green, borderWidth: 1, borderRadius: 3,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend: {{ display: false }} }},
    scales: {{
      x: {{ grid:{{color:'#ede3d4'}} }},
      y: {{ grid:{{display:false}}, ticks:{{font:{{size:10}}}} }}
    }}
  }}
}});
 
// ── THI monthly chart ─────────────────────────────
mk('thiMonthChart', {{
  type: 'line',
  data: {{
    labels: ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
    datasets: [
      {{ label:'Avg Max THI',  data:D.thi_monthly.map(r=>r.AvgMaxTHI),
         borderColor:C.red, tension:0.4, pointRadius:4, borderWidth:2.5,
         backgroundColor: C.red+'12', fill: true }},
      {{ label:'Avg Mean THI', data:D.thi_monthly.map(r=>r.AvgMeanTHI),
         borderColor:C.amber, tension:0.4, pointRadius:4, borderWidth:2,
         backgroundColor: 'transparent' }},
    ]
  }},
  options: {{ ...chartDefaults,
    scales: {{
      y: {{ grid:{{color:'#ede3d4'}}, min:20,
             title:{{display:true,text:'THI',color:C.muted}} }},
      x: {{ grid:{{display:false}} }}
    }}
  }}
}});
 
// ── THI stress chart ──────────────────────────────
mk('thiStressChart', {{
  type: 'bar',
  data: {{
    labels: D.thi_stats.map(r => farmName(r.HerdIdentifier)),
    datasets: [{{ label: 'Heat Stress Days (THI > 72)',
      data: D.thi_stats.map(r => r.HeatStressDays),
      backgroundColor: C.red + 'bb', borderColor: C.red,
      borderWidth: 1, borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend:{{display:false}} }},
    scales: {{
      y: {{ grid:{{color:'#ede3d4'}}, title:{{display:true,text:'Days',color:C.muted}} }},
      x: {{ grid:{{display:false}} }}
    }}
  }}
}});
 
// ── Repro chart ───────────────────────────────────
let reproChart;
let reproMetric = 'DaysOpen';
 
function updateReproChart(metric) {{
  reproMetric = metric;
  activateBtn(event.target);
  const keyMap = {{DaysOpen:'AvgDaysOpen', Interval:'AvgCalvingInterval', Inseminations:'AvgInseminations'}};
  const lblMap = {{DaysOpen:'Avg Days Open', Interval:'Avg Calving Interval (days)', Inseminations:'Avg Inseminations'}};
  const key = keyMap[metric];
  reproChart.data.datasets[0].data  = D.repro_stats.map(r => r[key] || 0);
  reproChart.data.datasets[0].label = lblMap[metric];
  reproChart.update('active');
}}
 
reproChart = mk('reproChart', {{
  type: 'bar',
  data: {{
    labels: D.repro_stats.map(r => farmName(r.HerdIdentifier)),
    datasets: [{{ label: 'Avg Days Open',
      data: D.repro_stats.map(r => r.AvgDaysOpen || 0),
      backgroundColor: C.amber + 'bb', borderColor: C.amber,
      borderWidth: 1, borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend:{{display:false}} }},
    scales: {{
      y: {{ grid:{{color:'#ede3d4'}} }},
      x: {{ grid:{{display:false}} }}
    }}
  }}
}});
 
// ── Insemination chart ────────────────────────────
mk('insemChart', {{
  type: 'bar',
  data: {{
    labels: ['0','1','2','3','4','5','6','7','8','9','10+'],
    datasets: [{{ label: 'Lactations',
      data: [57,578,377,226,130,77,53,32,23,9,6],
      backgroundColor: ['#c8dfc8','#3a6b4a','#5a9468','#8b6f47',
                         '#c17f2a','#e8a84a','#b03a2e','#b03a2e','#b03a2e','#b03a2e','#b03a2e'],
      borderWidth: 0, borderRadius: 4,
    }}]
  }},
  options: {{ ...chartDefaults,
    plugins: {{ ...chartDefaults.plugins, legend:{{display:false}} }},
    scales: {{
      x: {{ title:{{display:true,text:'Inseminations per lactation',color:C.muted}},
             grid:{{display:false}} }},
      y: {{ grid:{{color:'#ede3d4'}} }}
    }}
  }}
}});
 
// ── Rulebook bars ─────────────────────────────────
const rbEl = document.getElementById('rulebook-bars');
[['ReferenceData',64],['SensorData',7],['TemperatureHumidityIndex',13],['CalendarData',7],
 ...((D.milk_curve && D.milk_curve.length) ? [['MilkYield',12]] : [])].forEach(([ds,n]) => {{
  const pct = Math.round(n/64*100);
  rbEl.innerHTML += `
  <div style="margin-bottom:1rem;display:flex;align-items:center;gap:1rem">
    <div style="width:200px;font-size:0.82rem;color:var(--text);font-weight:500">${{ds}}</div>
    <div style="flex:1">
      <div style="height:8px;background:var(--warm2);border-radius:4px;overflow:hidden">
        <div style="height:100%;width:${{pct}}%;background:var(--green2);border-radius:4px;transition:width 0.8s ease"></div>
      </div>
    </div>
    <div style="width:60px;text-align:right;font-size:0.8rem;color:var(--muted)">${{n}} cols</div>
  </div>`;
}});
 
// ── Null profile bars ─────────────────────────────
const npEl = document.getElementById('null-profile-bars');
D.null_profile.forEach(r => {{
  const pct = parseFloat(r.NullPct);
  const cls = pct > 70 ? 'high' : pct > 30 ? 'mid' : 'low';
  const color = pct > 70 ? C.red : pct > 30 ? C.amber : C.green;
  npEl.innerHTML += `
  <div style="margin-bottom:1rem">
    <div style="display:flex;justify-content:space-between;margin-bottom:4px">
      <span style="font-size:0.82rem;color:var(--text);font-weight:500">${{r.Column}}</span>
      <span style="font-size:0.78rem;color:var(--muted)">${{pct}}% null</span>
    </div>
    <div class="null-bar">
      <div class="null-fill ${{cls}}" style="width:${{pct}}%"></div>
    </div>
  </div>`;
}});
 
// ── Animate bars on scroll ────────────────────────
const observer = new IntersectionObserver(entries => {{
  entries.forEach(e => {{
    if (e.isIntersecting) {{
      e.target.querySelectorAll('.null-fill, [style*="width:"]').forEach(el => {{
        el.style.transitionDelay = '0.1s';
      }});
    }}
  }});
}}, {{ threshold: 0.1 }});
 
document.querySelectorAll('section').forEach(s => observer.observe(s));

// ── Milk charts (only when the milk section is present) ──
if (D.milk_curve && D.milk_curve.length) {{
  const dims  = [...new Set(D.milk_curve.map(r => r.DaysInMilk))].sort((a,b) => a-b);
  const farms = [...new Set(D.milk_curve.map(r => r.HerdIdentifier))].sort();
  mk('milkCurveChart', {{
    type: 'line',
    data: {{ labels: dims, datasets: farms.map((f, i) => ({{
      label: farmName(f), borderColor: C.farms[i % C.farms.length], backgroundColor: 'transparent',
      borderWidth: 2, pointRadius: 0, tension: 0.3,
      data: dims.map(d => {{ const r = D.milk_curve.find(x => x.HerdIdentifier === f && x.DaysInMilk === d);
                             return r ? r.MeanMilkKg : null; }}) }})) }},
    options: {{ ...chartDefaults, scales: {{
      y: {{ grid:{{color:'#ede3d4'}}, title:{{display:true,text:'kg milk / day',color:C.muted}} }},
      x: {{ grid:{{display:false}}, title:{{display:true,text:'Days in milk',color:C.muted}}, ticks:{{maxTicksLimit:12}} }} }} }}
  }});
  mk('milkBehChart', {{
    type: 'line',
    data: {{ labels: D.milk_beh.map(r => r.DaysInMilk), datasets: [
      {{ label:'Milk (kg/d)', data:D.milk_beh.map(r => r.Milk), yAxisID:'y', borderColor:C.green,
         backgroundColor:'transparent', borderWidth:2.5, pointRadius:0, tension:0.3 }},
      {{ label:'Ruminating (min/d)', data:D.milk_beh.map(r => r.Ruminating), yAxisID:'y1', borderColor:C.amber,
         backgroundColor:'transparent', borderWidth:2, pointRadius:0, tension:0.3 }},
      {{ label:'Eating (min/d)', data:D.milk_beh.map(r => r.Eating), yAxisID:'y1', borderColor:C.brown2,
         backgroundColor:'transparent', borderWidth:2, pointRadius:0, tension:0.3, borderDash:[5,4] }} ] }},
    options: {{ ...chartDefaults, scales: {{
      y:  {{ position:'left',  grid:{{color:'#ede3d4'}}, title:{{display:true,text:'kg / day',color:C.muted}} }},
      y1: {{ position:'right', grid:{{display:false}},  title:{{display:true,text:'min / day',color:C.muted}} }},
      x:  {{ grid:{{display:false}}, title:{{display:true,text:'Days in milk',color:C.muted}}, ticks:{{maxTicksLimit:12}} }} }} }}
  }});
}}
 
</script>
</body>
</html>"""

# COMMAND ----------

with open('/tmp/SOS_Report.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f" HTML written — {len(html):,} bytes ({len(html)//1024} KB)")

# 1) Copy in the gold volume (for download / sharing inside Databricks)
shutil.copy('/tmp/SOS_Report.html', f"{GOLD_VOLUME}/SOS_Report.html")
print(f" Copied to: {GOLD_VOLUME}/SOS_Report.html")

# 2) GitHub Pages page: docs/index.html in this repository (when this notebook runs from the Git folder).
#    Commit and push docs/index.html from the Git folder afterwards; nothing is pushed from here.
docs_dir = os.path.normpath(os.path.join(os.getcwd(), "..", "..", "docs"))
if os.path.isdir(docs_dir):
    with open(os.path.join(docs_dir, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print(f" Written to the repository: {docs_dir}/index.html — commit it from the Git folder")
else:
    print(" Not running from the Git folder (no docs/ next to notebooks/): download the gold-volume copy "
          "and save it as docs/index.html in the repository.")
