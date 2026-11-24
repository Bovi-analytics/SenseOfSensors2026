# SenseOfSensors — Data Pipeline

**Institutions:** Utrecht University · Cornell University  
**Domain:** Precision dairy farming analytics  
**Data period:** 2016–2021 (re-hosted 2026)  
**Scale:** ~1.2 billion leg- and neck-sensor readings from 8 commercial dairy farms, plus daily milk recording

**Project page:** <https://bovi-analytics.github.io/SenseOfSensors2026/>  
**Data licence:** [CC BY 4.0](LICENSE-DATA) · **Code licence:** [MIT](LICENSE-CODE)

---

## Project overview

SenseOfSensors is a research data pipeline for precision dairy farming. It turns raw farm-management
exports, leg- and neck-sensor readings and milk recording into analysis-ready data, using the medallion architecture
(Bronze → Silver → Gold) on Databricks with Unity Catalog.

The data underpins published research on cow behaviour, heat stress adaptation, ketosis detection,
lameness and reproductive outcomes. Key findings include:

- Cows show heat-stress behavioural changes from as low as THI 56 (well below the previously accepted threshold of 72)
- Reduced eating time in weeks 3–4 before calving is linked to delayed rebreeding
- First-calving cows adapt more dramatically than older cows when entering the milking herd
- Clear links between lameness, lying time and body condition loss

---

## Released datasets

Two datasets are exported from the gold layer, each with one folder per farm (`Farm_A` … `Farm_H`):

| Dataset | Grain | Files per farm |
|---|---|---|
| **SOS24h** | one row per cow per day | `CowDay.csv`, `Lactation.csv`, `FarmClimateDay.csv` |
| **SOS2h** | one row per cow per day per 2-hour bin (12 per day) | `Cow2h.csv`, `Lactation.csv`, `FarmClimate2h.csv` |

SOS24h also has `MilkDay.csv` for the farms with milk recording: one row per cow per day (days 0–105 of each
lactation) with daily milk yield and, where measured, fat and protein. It links to `CowDay` on `CowID` + `EventDay`.

Each dataset has a `ReadMe.txt` (notes and build log) and a data dictionary
(`DataDictionarySOS24h.csv`, `DataDictionarySOS2h.csv`). The 15-minute daily totals in SOS24h are the sums
of the 2-hour bins in SOS2h, so the two datasets always agree.

The data files are not stored in this repository. They are released in two places:

## Get the data

| Where | What | Status |
|---|---|---|
| **bovidaeta** (Bovi-Analytics open data format) | SOS24h as `bovidaeta_catalog.senseofsensors_gold_sos24h`; SOS2h to follow | Being published — link added here when live |
| **Frontiers in Animal Science** (FAIR² Data Article) | SOS24h with a peer-reviewed data article and DOI | In preparation — DOI added here on publication |

**Notes on the bovidaeta version**
- Daily farm climate (THI, temperature, humidity, heat-stress hours) is not yet included: the format has no
  farm-level record. It is complete in the SOS24h / SOS2h files and will be added when the format supports it.
- Twin births are two Calving events (one per calf). Stillborn status and calving type are recorded for both
  calves, because the source does not say which calf they refer to.
- Blood calcium and BHBA are LabMeasurement events; health conditions (including those from the farm notes)
  are Diagnosis events. A few farm notes that only repeat another event (for example "calved") are not converted.
- Milk recording exists for 3 farms only (days 0–105 of each lactation, 2015–2018).

---

## Privacy and anonymisation

- **Bronze (raw) data is never shared.** It contains the original farm IDs, animal IDs and national life numbers.
- From silver onwards every dataset is anonymised: farm IDs are replaced by `Farm_A` … `Farm_H`, animal and
  life-number identifiers are pseudonymised with a secret key, and free-text fields that can contain
  identifiers are dropped.
- The anonymisation helper and its keys are kept in a private part of the Databricks workspace and are
  **not** part of this repository. The silver cleaning notebooks call it with `%run`; that call only works
  inside the project workspace.
- No passwords, tokens, keys or salts are stored in this repository.

---

## Repository structure

```
notebooks/
├── 1_Bronze/
│   ├── 1.1 SOS-BronzeLayer.py                          Raw ingestion of all source files (read-only afterwards)
│   ├── 1.2 SOS-DataProfile.py                          Profiling reports of the raw data (kept private)
│   └── 1.4 SOS-MilkYieldBronze.py                      Read-only check of the uploaded milk workbook
│
├── 2_Silver/
│   ├── 2.11 SOS-RefDataRulebookSilverLayer.py          ReferenceData rulebook (metadata tables)
│   ├── 2.12 SOS-RefDataCleaningConstraintsSilverLayer.py  ReferenceData cleaning → silver
│   ├── 2.21 SOS-THIDataRulebookSilverLayer.py          THI rulebook
│   ├── 2.22 SOS-THIDataCleaningConstraintSilverLayer.py   THI cleaning → silver
│   ├── 2.31 SOS-SensorDataRulebookSilverLayer.py       SensorData rulebook
│   ├── 2.32 SOS-SensorDataCleaningConstraintSilverLayer.py  SensorData cleaning → silver (~1.2B rows)
│   ├── 2.41 SOS-CalendarDataRulebookSilverLayer.py     CalendarData rulebook
│   ├── 2.42 SOS-CalendarDataSilverLayer.py             CalendarData cleaning → silver
│   ├── 2.5  SOS-RulebookCleaningDownloaded.py          Export all metadata tables to one Excel workbook
│   ├── 2.61 SOS-MilkYieldRulebookSilverLayer.py        MilkYield rulebook
│   └── 2.62 SOS-MilkYieldSilverLayer.py                MilkYield cleaning → silver (farm matched by life numbers)
│
├── 3_Gold/
│   ├── 3.1  SOS-GoldLayerInitialAnalysis.py            Checks across the silver datasets
│   ├── 3.21 SOS-GoldLayer1LactationWindow.py           Gold 1 — LactationWindows
│   ├── 3.22 SOS-GoldLayer2AnimalIdentityBridge.py      Gold 2 — AnimalIdentityBridge
│   ├── 3.33 SOS-GoldLayer3DailyBehaviorSummary.py      Gold 3 — DailyBehaviourSummary
│   ├── 3.34 SOS-GoldLayer4DailyAnimalIdentity.py       Gold 4 — DailyAnimalIdentity
│   ├── 3.35 SOS-GoldLayer5DailyTHISummary.py           Gold 5 — DailyTHISummary
│   ├── 3.36 SOS-GoldLayer6ClinicalFlags.py             Gold 6 — ClinicalFlags
│   ├── 3.37 SOS-GoldLayer7ReproductiveTimeline.py      Gold 7 — ReproductiveTimeline
│   ├── 3.38 SOS-GoldLayer8GoldMaster.py                Gold 8 — GoldMaster
│   ├── 3.39 SOS-GoldLayer9Migration.py                 Copy gold tables to the gold volume
│   ├── 3.40 SOS-GoldLayer10DailyMilkYield.py           Gold 10 — DailyMilkYield
│   ├── 3.41 SOS-GoldLayer9Validation1.py               Replicates published findings as a validation
│   ├── 3.5  SOS-GoldExportDatasets.py                  Builds the released SOS24h and SOS2h datasets
│   ├── 3.6  SOS-GoldExportMilk.py                      Adds MilkDay files to SOS24h (no full rebuild)
│   └── 3.7  SOS-GoldReleaseDocs.py                     Licence/citation in the ReadMe files; fills data dictionary gaps
│
└── 4_Report/
    └── 4 SOS-HTMLFinalReport.py                        Builds the project page (writes docs/index.html)

docs/          GitHub Pages site (project report + links to data and code)
LICENSE-CODE   MIT licence for the code
LICENSE-DATA   CC BY 4.0 licence for the data
CITATION.cff   How to cite (GitHub "Cite this repository")
```

The notebooks are Databricks source files (`.py` with `# COMMAND ----------` cell separators). Import them
into a Databricks workspace, or open them as plain Python to read them.

---

## Architecture

### Storage

| Layer | Location | Format |
|---|---|---|
| Bronze | `/Volumes/senseofsensors_catalog/bronze/bronze_volume/` | Parquet (ReferenceData, SensorData), CSV (THI, CalendarData) |
| Silver | `/Volumes/senseofsensors_catalog/silver/silver_volume/` | Delta (SensorData, THI, ReferenceData) |
| Silver metadata | `senseofsensors_catalog.silver.*` | Managed Delta tables |
| Gold | `/Volumes/senseofsensors_catalog/gold/gold_volume/` | Delta tables; `SOS24h/` and `SOS2h/` exports (CSV) |

### Medallion layers

**Bronze** — Raw data, read-only. Never modified. Always available as the source of truth.

**Silver** — Cleaned, standardised, typed and anonymised. Every row is kept where possible; problems become
warnings in a `Warnings` array column, and rows are dropped only when clearly corrupt (e.g. dates before 2000).
No data is silently discarded.

**Gold** — Analysis-ready joins, aggregations and derived research variables, saved as Delta tables and
exported as the SOS24h and SOS2h datasets.

---

## Source datasets

### 1. ReferenceData

Clinical and management data collected at calving for each cow-lactation record.

| Property | Value |
|---|---|
| Source format | Parquet (1 file, 63 positionally-named columns) |
| Bronze rows | ~1,669 |
| Silver rows | ~1,640 (ghost rows removed) |
| Dedup key | HerdIdentifier + AnimalIdentifier + LifeNumber |

**Column groups:** Identity, DryPeriod, Calving, Calcium, Ketosis (BHBA), ReproWeek4, ReproWeek8, Attention (1–4), MPR

**Key cleaning decisions:**
- `CalvingDurationHr` — decimal hours; values with clock-time patterns above 6 h flagged `SuspectedClockTime` but kept
- `BcsStartDryPeriod` — values above 5 capped at 5.0 (farm scale is 1–5)
- `CycleWeek8` — free-text pregnancy phrases moved to `ClinicalObservationsOverflow`; numeric part kept
- `IndividualAttention1–4` — free text; flagged in gold through the translation glossary
- Non-numeric values in typed columns are moved to `ClinicalObservationsOverflow` as `"SourceColumn:value; …"`

### 2. TemperatureHumidityIndex (THI)

Hourly indoor and outdoor temperature, humidity and THI for each farm.

| Property | Value |
|---|---|
| Source format | CSV (semicolon-delimited, decimal comma) |
| Bronze rows | 345,869 |
| Dedup key | HerdIdentifier + MeasurementDatetime |

Derived in silver: `MeasurementDate`, `MeasurementHour`, `THIExternalStatus`, `THIInternalStatus`
(NoStress < 50 · Mild 50–72 · HeatStress 72–80 · SevereHeatStress > 80).

**THI formula (Ravagnolo):** `THI = (1.8×T + 32) − (0.55 − 0.0055×RH) × (1.8×T − 26)`

**Coverage:** 2017-01-01 to 2020-11-03 for all 8 farms. Internal columns are null for external-only farms (~52% of rows).

### 3. SensorData

Leg- and neck-sensor readings for all animals on all farms (each cow wore one of each).

| Property | Value |
|---|---|
| Source format | Parquet (23 sub-folders, one per ObservationType) |
| Bronze rows | 1,214,421,979 |
| Silver rows | 1,204,964,611 |
| Partition | By `ObservationType` (35 partitions) |
| Dedup key | HerdIdentifier + AnimalIdentifier + EventDate + ObservationType + ObservationValue |

Time-budget measurements (eating, ruminating, inactive, lying, standing, walking) at 15-minute, 2-hour and
daily resolution, plus bout statistics and leg-activity counts. `ObservationType` names are normalised to
PascalCase; `AttributeMap` keys are normalised to PascalCase and kept as a map in silver.

### 4. CalendarData

Farm management events from the herd management system: calving, dry-off, insemination, heat, pregnancy checks.

| Property | Value |
|---|---|
| Source format | CSV (two overlapping batches) |
| Bronze rows | 85,578 (before dedup across batches) |
| Silver rows | 48,814 |
| Dedup key | HerdIdentifier + AnimalIdentifier + EventDate + ObservationType |

Dates before 2000 are dropped as corrupt; `DryoffEvent` dates replace estimated lactation ends in Gold 1.
**Coverage:** 2012–2020 across all 8 farms.

### 5. MilkYield

Daily milk recording from the farms' milking systems: one workbook with one sheet per farm.

| Property | Value |
|---|---|
| Source format | Excel workbook (one sheet per farm), uploaded unchanged to `bronze_volume/MilkYield/` |
| Bronze rows | ~126,000 (3 farms) |
| Coverage | 2015–2018; days 0–105 of each lactation |
| Columns | cow number, life number, lactation number, calving date, previous dry-off date, date, days in milk, daily milk (kg), fat %, protein % |

**Key cleaning decisions:**
- Sheet names are farm names and are never stored. Each sheet is matched to a farm by the share of its cows' (hashed)
  life numbers found on that farm in the other datasets; the notebook stops if the match is weak or ambiguous.
- Some cow-days are stored twice with near-identical yields. These are repeated records, not two milkings (their mean
  matches the neighbouring days, their sum does not): flagged in silver, averaged in gold.
- 0 kg milk is kept in silver with a warning and treated as missing in gold; fat/protein of 0 % means not measured.
- `Dry off date` always falls before the calving date, so it is the dry-off ending the previous lactation
  (`PreviousDryOffDate`, with `DryPeriodDays`).

---

## Shared metadata tables (rules as data)

Five Delta tables in `senseofsensors_catalog.silver` declare the cleaning logic, so it can be audited and
changed without touching pipeline code. Each has a `Dataset` column.

| Table | Purpose |
|---|---|
| `Rulebook` | One row per column: source name, target name, type, range, strategy |
| `CleaningConstraints` | Cross-field checks, dedup keys, blank tokens, date formats |
| `CodeMappings` | Code → English label (e.g. CalvingType 1 = Normal) |
| `TranslationGlossary` | Farm-note term → English translation + condition-flag column |
| `UnitConversionLookup` | Unit conversions (mmol/L ↔ mg/dL) |

---

## Gold tables

| Gold | Table | Grain | Purpose |
|---|---|---|---|
| 1 | LactationWindows | cow-lactation | Lactation start/end (actual dry-off dates where available), clinical and composite flags |
| 2 | AnimalIdentityBridge | sensor animal | Links sensor animals to ReferenceData; resolves life-number formats |
| 3 | DailyBehaviourSummary | animal-day | 1.2B sensor rows pivoted to daily time budgets; 3-SD outlier filter; days > 1440 min nulled |
| 4 | DailyAnimalIdentity | animal-day | Handles reused cow numbers and sensors moved between cows |
| 5 | DailyTHISummary | farm-day | Daily THI range, heat-stress hours and cumulative heat load |
| 6 | ClinicalFlags | cow-lactation | 61 boolean condition flags from free-text farm notes via the glossary |
| 7 | ReproductiveTimeline | cow-lactation | Insemination, heat, pregnancy check, dry-off, next calving, days open |
| 8 | GoldMaster | animal-day | All of the above joined with left joins; ~2.5M rows, 158 columns |
| 10 | DailyMilkYield | cow-day | Daily milk, fat and protein; repeated records averaged; linked to GoldMaster by life number |

**LactationPhase values:** PreCalving (< 0 DIM), EarlyTransition (0–21), EarlyLactation (22–60),
MidLactation (61–150), LateLactation (151–305), Extended (> 305).

---

## Running order

```
1.1  Bronze ingestion
2.11 → 2.12   ReferenceData rulebook, then cleaning
2.21 → 2.22   THI rulebook, then cleaning
2.31 → 2.32   SensorData rulebook, then cleaning (heavy)
2.41 → 2.42   CalendarData rulebook, then cleaning
2.5           Metadata export to Excel (any time after the rulebooks)
1.4 → 2.61 → 2.62   MilkYield check, rulebook, cleaning (after ReferenceData silver)
3.21, 3.22    Gold 1 and 2 (needed by the later gold tables)
3.33 – 3.37   Gold 3–7 (Gold 3 is heavy, 20–40 min)
3.38          Gold 8 — GoldMaster
3.39          Copy gold tables to the gold volume
3.41          Validation against published findings
3.40          Gold 10 — DailyMilkYield (after GoldMaster)
3.5           Export SOS24h and SOS2h
3.6           Add MilkDay files to SOS24h (re-run after every 3.5)
3.7           Release documentation: licence and citation in ReadMe, data dictionary gaps (re-run after 3.5/3.6)
4             Project page (docs/index.html)
```

---

## Conventions

- **Naming:** PascalCase for all column, folder and metadata names; units in names where useful
  (`TempExternalCelsius`, `DailyEatingMin15`); acronyms in capitals (`MPR`, `DIM`, `BHBA`, `THI`).
- **Dates:** date-only values use Spark `date`, date-times use `timestamp`; silver writes `yyyy-MM-dd`.
- **Warnings:** every silver row has `Warnings: array<string>` and `WarningCount: int`, formatted
  `WarningType:ColumnName (action taken)`.
- **Nulls:** null means no data; never zero unless the cow genuinely had zero activity.
- **Bronze is read-only.** No notebook writes to the bronze volume.

---

## Data quality notes

- **THI coverage gap:** THI starts 2017-01-01; sensor data starts 2016-01-01, so 2016 sensor days have no THI.
- **Sensor reassignment:** when a cow leaves the herd her leg sensor is given to another cow. Dry-off dates
  bound the lactations; sensor-only animals (no ReferenceData) are kept and flagged `HasRefData = false`.
- **Animal identity:** 31.7% of sensor animals (1,629 of 5,143) have no ReferenceData record; they were in the
  herd during the sensor period but outside the clinical study cohort (2016–2018 calvings).
- **Placeholder animal:** one farm uses a placeholder animal record with parity 9999; it is kept with a null
  life number and excluded from animal-level analysis.
- **Foreign-registered cow:** one animal has a different country prefix in its life number than all other
  cows and does not match the ReferenceData entries.

---

## How to cite

The data are licensed under CC BY 4.0, which requires credit to the source. Please cite:

1. the dataset publication (reference and DOI added on publication),
2. the original SenseOfSensors study: Hut et al. (2022), PLOS ONE,
3. this repository (GitHub's "Cite this repository" button uses [CITATION.cff](CITATION.cff)).

## Acknowledgements

The SenseOfSensors dataset was made available for open science with the agreement of the project partners: Peter R. Hut, Elsbeth Stassen, Mirjam Nielen, Gerrit Koop, Jan Hulsen and Arnold Harbers. Pipeline and release: Lipika Mallikarjuna and Miel Hostens (Bovi-Analytics lab, Cornell University).

---

## Licences

- **Code** (notebooks and scripts): [MIT](LICENSE-CODE)
- **Data** (SOS24h, SOS2h, data dictionaries, report figures): [CC BY 4.0](LICENSE-DATA)

---

## Contact

Bovi-analytics — SenseOfSensors research project, Cornell University.
Please open a GitHub issue for questions about the code or data.
