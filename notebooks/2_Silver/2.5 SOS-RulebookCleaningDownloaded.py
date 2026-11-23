# Databricks notebook source
# MAGIC %pip install openpyxl==3.0.10

# COMMAND ----------

# SOS — Combined Rulebook & Metadata Export
# Reads all five shared silver metadata tables and writes one Excel workbook.
# Tabs: Rulebook + CleaningConstraints per dataset, plus shared lookup tables.
# Run after all three rulebook notebooks have completed.

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment

CATALOG       = "senseofsensors_catalog"
SILVER_SCHEMA = f"{CATALOG}.silver"
DATASETS = ["ReferenceData", "TemperatureHumidityIndex", "SensorData", "CalendarData"]

# 1. Pull per-dataset tables
rulebook_dfs    = {}
constraint_dfs  = {}

for ds in DATASETS:
    rulebook_dfs[ds] = (
        spark.table(f"{SILVER_SCHEMA}.Rulebook")
             .where(f"Dataset = '{ds}'")
             .orderBy("Ordinal")
             .toPandas()
    )
    constraint_dfs[ds] = (
        spark.table(f"{SILVER_SCHEMA}.CleaningConstraints")
             .where(f"Dataset = '{ds}'")
             .toPandas()
    )
    print(f"{ds}: {len(rulebook_dfs[ds])} rules, {len(constraint_dfs[ds])} constraints")

# 2. Pull shared lookup tables (all datasets combined, Dataset column included)
code_mappings_df = (
    spark.table(f"{SILVER_SCHEMA}.CodeMappings")
         .orderBy("Dataset", "Column", "Code")
         .toPandas()
)
glossary_df = (
    spark.table(f"{SILVER_SCHEMA}.TranslationGlossary")
         .orderBy("Dataset", "Category", "TermNl")
         .toPandas()
)
unit_conv_df = (
    spark.table(f"{SILVER_SCHEMA}.UnitConversionLookup")
         .orderBy("Dataset", "ColumnName")
         .toPandas()
)
print(f"CodeMappings: {len(code_mappings_df)} rows")
print(f"TranslationGlossary: {len(glossary_df)} rows")
print(f"UnitConversionLookup: {len(unit_conv_df)} rows")

# 3. Short names for tab labels (Excel tab names max 31 chars)
SHORT = {
    "ReferenceData":            "RefData",
    "TemperatureHumidityIndex": "THI",
    "SensorData":               "Sensor",
    "CalendarData":             "Calendar",
}

# 4. Write to Excel
tmp = "/tmp/SOS-Combined-Rulebook.xlsx"

with pd.ExcelWriter(tmp, engine="openpyxl") as xl:
    for ds in DATASETS:
        s = SHORT[ds]
        rulebook_dfs[ds].to_excel(xl,   sheet_name=f"{s} Rules",       index=False)
        constraint_dfs[ds].to_excel(xl, sheet_name=f"{s} Constraints", index=False)
    code_mappings_df.to_excel(xl, sheet_name="CodeMappings",        index=False)
    glossary_df.to_excel(xl,      sheet_name="TranslationGlossary", index=False)
    unit_conv_df.to_excel(xl,     sheet_name="UnitConversions",     index=False)

# 5. Style — header colour, auto-width, freeze, filter
wb = load_workbook(tmp)

HEADER_FILL = PatternFill("solid", fgColor="1F4E79")
HEADER_FONT = Font(color="FFFFFF", bold=True, size=11)

# Tab colours by group so it's easy to navigate
TAB_COLOURS = {
    "RefData Rules":       "2E75B6",
    "RefData Constraints": "2E75B6",
    "THI Rules":           "375623",
    "THI Constraints":     "375623",
    "Sensor Rules":        "7B2C2C",
    "Sensor Constraints":  "7B2C2C",
    "Calendar Rules":      "C45911",
    "Calendar Constraints":"C45911",
    "CodeMappings":        "7030A0",
    "TranslationGlossary": "7030A0",
    "UnitConversions":     "7030A0",
}

for ws in wb.worksheets:
    # Header row
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", wrap_text=True)
    # Auto-width
    for col in ws.columns:
        max_len = max((len(str(c.value)) if c.value else 0) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max(max_len + 3, 14), 70)
    # Freeze header and enable filter
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    # Tab colour
    colour = TAB_COLOURS.get(ws.title)
    if colour:
        ws.sheet_properties.tabColor = colour

wb.save(tmp)

# 6. Copy to Volume for download
import shutil, os
VOLUME_PATH = "/Volumes/senseofsensors_catalog/silver/silver_volume/SOS-Combined-Rulebook.xlsx"
os.makedirs(os.path.dirname(VOLUME_PATH), exist_ok=True)
shutil.copy(tmp, VOLUME_PATH)
print(f"Excel exported to: {VOLUME_PATH}")
print("Download from the Volumes browser in the Catalog UI.")

# 7. Summary
print("\n" + "=" * 50)
print("Workbook tabs:")
for ws_name in wb.sheetnames:
    n = len(wb[ws_name].max_row) if hasattr(wb[ws_name].max_row, '__len__') else wb[ws_name].max_row
    print(f"  {ws_name:<28} {wb[ws_name].max_row - 1:>5} rows")
print("=" * 50)