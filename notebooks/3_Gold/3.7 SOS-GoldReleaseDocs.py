# Databricks notebook source
# MAGIC %md
# MAGIC # SOS — release documentation for SOS24h and SOS2h
# MAGIC Run after `3.5` (export) and `3.6` (milk). Changes only the two documentation files of each dataset:
# MAGIC
# MAGIC * `ReadMe.txt` — adds a "Licence, citation and contact" section at the top (replaced on re-run).
# MAGIC * `DataDictionary….csv` — fills **empty** units and descriptions from the list below. Existing text is never
# MAGIC   overwritten. Columns that are still empty afterwards are printed, so they can be added to `FILL`.
# MAGIC
# MAGIC The data files themselves are not touched.

# COMMAND ----------

dbutils.widgets.text("Catalog", "senseofsensors_catalog")
dbutils.widgets.text("DatasetDoi", "")      # fill in once Frontiers assigns one
CATALOG = dbutils.widgets.get("Catalog")
DOI     = dbutils.widgets.get("DatasetDoi").strip()
GOLD    = f"/Volumes/{CATALOG}/gold/gold_volume"
REPO    = "https://github.com/Bovi-analytics/SenseOfSensors2026"

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## 1 — Licence, citation and contact in ReadMe.txt

# COMMAND ----------

HEADER = "Licence, citation and contact"
dataset_cite = (f"SenseOfSensors dataset, https://doi.org/{DOI}" if DOI
                else "SenseOfSensors dataset (Frontiers in Animal Science FAIR² Data Article; DOI added on publication)")
block = [
    HEADER, "-" * len(HEADER),
    "Licence: Creative Commons Attribution 4.0 International (CC BY 4.0), https://creativecommons.org/licenses/by/4.0/",
    "You may share and adapt the data for any purpose, provided you give appropriate credit.",
    "",
    "Please cite:",
    f"  1. {dataset_cite}",
    "  2. Hut PR, Kuiper SEM, Nielen M, Hulsen JHJL, Stassen EN, Hostens MM (2022). Sensor based time budgets in "
    "commercial Dutch dairy herds vary over lactation cycles and within 24 hours. PLOS ONE. "
    "https://doi.org/10.1371/journal.pone.0264392",
    "",
    f"Code, documentation and project page: {REPO}  ·  https://bovi-analytics.github.io/SenseOfSensors2026/",
    f"Questions: open an issue at {REPO}/issues (Bovi-Analytics lab, Cornell University).",
    "Privacy: farm, animal and life-number identifiers are pseudonymised. Do not attempt to re-identify farms or animals.",
    "",
]

def add_block(path):
    txt = dbutils.fs.head(path, 10_000_000)
    if HEADER in txt:                                   # replace an earlier version of the block
        start = txt.index(HEADER)
        end = txt.find("\n\n", txt.find("Privacy:", start))
        txt = txt[:start] + txt[end + 2:] if end != -1 else txt[:start]
    lines = txt.split("\n")
    # put the block right after the title, the "=====" line and the Built/Farms lines
    cut = next((i for i, l in enumerate(lines) if l.strip() == "" and i > 1), 2) + 1
    new = "\n".join(lines[:cut] + block + lines[cut:])
    dbutils.fs.put(path, new, overwrite=True)
    print(f"  {path}: licence block added")

for ds in ["SOS24h", "SOS2h"]:
    add_block(f"{GOLD}/{ds}/ReadMe.txt")

# COMMAND ----------

# MAGIC %md ## 2 — Fill empty units and descriptions in the data dictionaries

# COMMAND ----------

FILL = {   # Column: (unit, description) — used only where the dictionary is empty; definitions as in the gold notebooks
    "CalvingType": ("code", "Calving ease code from the farm records (3 = difficult, 4 = caesarean; see CalvingTypeLabel)"),
    "CalvingTypeLabel": ("", "Calving ease in words"),
    "Afterbirth": ("code", "Afterbirth (placenta) code from the farm records (2-4 = retained; see AfterbirthLabel)"),
    "AfterbirthLabel": ("", "Afterbirth code in words"),
    "CalfRemoval": ("code", "Calf removal code from the farm records (see CalfRemovalLabel)"),
    "CalfRemovalLabel": ("", "Calf removal code in words"),
    "CalvingDurationHr": ("h", "Duration of calving"),
    "Repro4Uterus": ("code", "Uterus finding at the reproductive check around week 4 after calving"),
    "Repro8Uterus": ("code", "Uterus finding at the reproductive check around week 8 after calving"),
    "Repro4UterusLabel": ("", "Uterus finding around week 4, in words"),
    "Repro8UterusLabel": ("", "Uterus finding around week 8, in words"),
    "SubclinicalKetosis": ("boolean", "BHBA >= 1.2 mmol/L in either blood sample (BHBA1 or BHBA2)"),
    "SubclinicalHypocalcaemia": ("boolean", "Blood calcium < 2.0 mmol/L"),
    "DifficultCalving": ("boolean", "CalvingType 3 (difficult) or 4 (caesarean)"),
    "RetainedPlacenta": ("boolean", "Afterbirth code 2, 3 or 4 (placenta retained)"),
    "BcsChangeDryPeriod": ("score", "BcsEndDryPeriod minus BcsStartDryPeriod; negative = condition lost over the dry period"),
    "InseminationCount": ("count", "Number of inseminations in this lactation"),
    "KeepOpenCount": ("count", "Number of 'keep open' (do not breed) registrations in this lactation"),
    "DaysOpen": ("d", "Days from calving to the last insemination (proxy for days to conception)"),
    "CalvingIntervalDays": ("d", "Days from this calving to the next calving"),
    "ConceptionAtFirstInsemination": ("boolean", "Exactly one insemination in this lactation (InseminationCount = 1)"),
    "ReproductiveSuccess": ("boolean", "A next calving is recorded for this lactation"),
    "EarlyFirstInsemination": ("boolean", "First insemination before 50 days in milk"),
    "DailyLegActivity": ("sensor units/d", "Leg activity, daily total"),
    "MaxTHIExternal": ("index", "Highest outdoor temperature-humidity index (THI) of the day"),
    "MinTHIExternal": ("index", "Lowest outdoor THI of the day"),
    "MeanTHIExternal": ("index", "Mean outdoor THI of the day"),
    "HeatStressHoursExternal": ("h", "Hourly readings with outdoor THI > 72"),
    "SevereHeatStressHoursExternal": ("h", "Hourly readings with outdoor THI > 80"),
    "CumulativeHeatLoadExternal": ("THI-h", "Sum of (THI - 72) over hourly readings with outdoor THI > 72"),
    "DailyTHIStatusExternal": ("", "Heat-stress class from the day's maximum outdoor THI: <50 NoStress, 50-72 Mild, "
                                   ">72 HeatStress, >80 SevereHeatStress"),
}

def fill_dictionary(path):
    d = spark.read.option("header", True).csv(path).toPandas()
    before = ((d["Unit"].isna()) | (d["Description"].isna())).sum()
    for i, r in d.iterrows():
        unit, desc = FILL.get(r["Column"], (None, None))
        if desc and (r["Description"] is None or str(r["Description"]).strip() in ("", "nan")):
            d.at[i, "Description"] = desc
        if unit and (r["Unit"] is None or str(r["Unit"]).strip() in ("", "nan")):
            d.at[i, "Unit"] = unit
    d = d.fillna("")
    tmp = path + "__tmp"
    spark.createDataFrame(d.astype(str)).repartition(1).write.mode("overwrite").option("header", True).csv(tmp)
    part = [f for f in dbutils.fs.ls(tmp) if f.name.startswith("part-")][0]
    dbutils.fs.rm(path)
    dbutils.fs.mv(part.path, path)
    dbutils.fs.rm(tmp, recurse=True)
    still = d[(d["Description"].str.strip() == "")][["File", "Column", "Type"]]
    print(f"  {path}: {before} rows had a gap before; {len(still)} columns still without a description")
    return still

left = []
for ds, name in [("SOS24h", "DataDictionarySOS24h.csv"), ("SOS2h", "DataDictionarySOS2h.csv")]:
    left.append(fill_dictionary(f"{GOLD}/{ds}/{name}").assign(Dataset=ds))

import pandas as pd
left = pd.concat(left)
print("\nColumns still without a description (send this list to add them to FILL):")
print(left.to_string(index=False) if len(left) else "  none")
