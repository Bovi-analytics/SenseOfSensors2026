# Databricks notebook source
# MAGIC %pip install openpyxl==3.1.2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("Catalog", "senseofsensors_catalog")
CATALOG    = dbutils.widgets.get("Catalog").strip()
BRONZE_DIR = f"/Volumes/{CATALOG}/bronze/bronze_volume/MilkYield"

EXPECTED = ["Usernumber", "Life number", "Lactation number", "Calving date", "Dry off date", "Date",
            "Lactation day", "Day production", "Fat percentage", "Protein percentage"]

files = [f for f in dbutils.fs.ls(BRONZE_DIR) if f.name.lower().endswith(".xlsx")]
if len(files) != 1:
    raise ValueError(f"Expected exactly one .xlsx in {BRONZE_DIR}, found {[f.name for f in files]}")
SOURCE = files[0].path.replace("dbfs:", "")
print(f"Source workbook: {files[0].name}  ({files[0].size/1e6:,.1f} MB)")

# COMMAND ----------

import pandas as pd

sheets = pd.read_excel(SOURCE, sheet_name=None, dtype=str)
print(f"Sheets (= farms): {len(sheets)}\n")
problems = []
for i, (name, df) in enumerate(sheets.items(), 1):
    cols = [c.strip() for c in df.columns]
    missing, extra = sorted(set(EXPECTED) - set(cols)), sorted(set(cols) - set(EXPECTED))
    dates = pd.to_datetime(df["Date"], errors="coerce") if "Date" in cols else pd.Series(dtype="datetime64[ns]")
    print(f"Sheet {i}: {len(df):>7,} rows · {df['Life number'].nunique():>4} cows · "
          f"{dates.min():%Y-%m-%d} → {dates.max():%Y-%m-%d}")
    if missing or extra:
        problems.append(f"Sheet {i}: missing {missing}, unexpected {extra}")

print()
print("\n".join(problems) if problems else "All sheets have the expected 10 columns.")

# COMMAND ----------

# Raw value checks that silver will handle (reported here so surprises are seen before cleaning)
for i, (name, df) in enumerate(sheets.items(), 1):
    d = df.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    rep = d.duplicated(["Life number", "Date"]).sum()
    print(f"Sheet {i}: no life number {d['Life number'].isna().sum():,} · repeated cow-days {rep:,} · "
          f"milk = 0 kg {(pd.to_numeric(d['Day production'], errors='coerce') == 0).sum():,} · "
          f"fat missing {d['Fat percentage'].isna().mean():.0%}")