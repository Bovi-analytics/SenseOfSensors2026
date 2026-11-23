# Databricks notebook source
# MAGIC %pip install ydata-profiling

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from ydata_profiling import ProfileReport

# Profiles of raw bronze data contain real identifiers: they are written to the private
# confidential volume, never to bronze (bronze is read-only) and never shared.
spark.sql("CREATE VOLUME IF NOT EXISTS senseofsensors_catalog.confidential.profiles")
PROFILE_DIR = "/Volumes/senseofsensors_catalog/confidential/profiles"

# Read ReferenceData
reference_path = "/Volumes/senseofsensors_catalog/bronze/bronze_volume/ReferenceData/"
reference_df = spark.read.parquet(reference_path)

# Convert all data to Pandas
reference_pdf = reference_df.toPandas()

# Create profile report
profile = ProfileReport(
    reference_pdf,
    title="ReferenceData Profile Report",
    explorative=True
)

# Display the HTML report in Databricks
displayHTML(profile.to_html())

# COMMAND ----------

profile.to_file(f"{PROFILE_DIR}/ReferenceData_Profile.html")

# COMMAND ----------

from ydata_profiling import ProfileReport

# Read THI data
THI_path = "/Volumes/senseofsensors_catalog/bronze/bronze_volume/TemperatureHumidityIndex/"

THI_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .csv(THI_path)

# Check columns
print(THI_df.columns)

# Convert to Pandas
THI_pdf = THI_df.toPandas()

# Create profile report
profile = ProfileReport(
    THI_pdf,
    title="THIData Profile Report",
    explorative=True
)

# Save report
profile.to_file(
    f"{PROFILE_DIR}/THIData_Profile.html"
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import html


# 1. READ FULL SENSOR DATA

sensor_path = "/Volumes/senseofsensors_catalog/bronze/bronze_volume/SensorData/"

sensor_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(sensor_path)
)

total_rows = sensor_df.count()

print(f"Total rows: {total_rows:,}")
print(f"Columns: {len(sensor_df.columns)}")


# 2. BASIC COLUMN INFORMATION

column_info = []

for field in sensor_df.schema.fields:

    column_info.append({
        "Column": field.name,
        "Data Type": str(field.dataType),
        "Nullable": field.nullable
    })

column_pdf = pd.DataFrame(column_info)


# 3. MISSING VALUES

missing_expressions = []

for c in sensor_df.columns:

    if c in [
        "AnimalIdentifier",
        "HerdIdentifier",
        "ObservationType",
        "ObservationValue"
    ]:

        missing_expressions.append(
            F.sum(
                F.when(
                    F.col(c).isNull() |
                    (F.trim(F.col(c)) == ""),
                    1
                ).otherwise(0)
            ).alias(c)
        )

    else:

        missing_expressions.append(
            F.sum(
                F.when(F.col(c).isNull(), 1)
                .otherwise(0)
            ).alias(c)
        )


missing_row = sensor_df.agg(
    *missing_expressions
).collect()[0]


missing_results = []

for c in sensor_df.columns:

    missing = int(missing_row[c])
    non_missing = total_rows - missing
    missing_pct = (missing / total_rows) * 100

    missing_results.append({
        "Column": c,
        "Total Rows": total_rows,
        "Non-Missing": non_missing,
        "Missing": missing,
        "Missing %": round(missing_pct, 3)
    })


missing_pdf = pd.DataFrame(missing_results)


# 4. UNIQUE VALUE COUNTS

unique_results = []

for c in [
    "AnimalIdentifier",
    "HerdIdentifier",
    "ObservationType",
    "ObservationValue"
]:

    result = sensor_df.select(
        F.approx_count_distinct(c).alias("unique")
    ).collect()[0]["unique"]

    unique_results.append({
        "Column": c,
        "Approx. Unique Values": result
    })


unique_pdf = pd.DataFrame(unique_results)


# 5. EVENT DATE INFORMATION

date_stats = sensor_df.agg(
    F.min("EventDate").alias("Minimum"),
    F.max("EventDate").alias("Maximum"),
    F.count("EventDate").alias("NonMissing")
).collect()[0]

date_pdf = pd.DataFrame([{
    "Minimum EventDate": str(date_stats["Minimum"]),
    "Maximum EventDate": str(date_stats["Maximum"]),
    "Non-Missing EventDate": int(date_stats["NonMissing"]),
    "Missing EventDate": total_rows - int(date_stats["NonMissing"])
}])


# 6. OBSERVATION TYPE DISTRIBUTION

observation_type_df = (
    sensor_df
    .groupBy("ObservationType")
    .count()
    .withColumn(
        "Percentage",
        F.col("count") / F.lit(total_rows) * 100
    )
    .orderBy(F.desc("count"))
)

observation_type_pdf = observation_type_df.toPandas()

observation_type_pdf.columns = [
    "ObservationType",
    "Count",
    "Percentage"
]

observation_type_pdf["Percentage"] = (
    observation_type_pdf["Percentage"].round(3)
)


# 7. OBSERVATION VALUE CHARACTERISTICS

# Determine whether ObservationValue can be interpreted as numeric
observation_value_characteristics = sensor_df.select(

    F.sum(
        F.when(
            F.col("ObservationValue").isNull() |
            (F.trim(F.col("ObservationValue")) == ""),
            1
        ).otherwise(0)
    ).alias("Missing"),

    F.sum(
        F.when(
            F.col("ObservationValue").isNotNull() &
            (F.trim(F.col("ObservationValue")) != "") &
            F.col("ObservationValue").cast("double").isNotNull(),
            1
        ).otherwise(0)
    ).alias("Numeric"),

    F.sum(
        F.when(
            F.col("ObservationValue").isNotNull() &
            (F.trim(F.col("ObservationValue")) != "") &
            F.col("ObservationValue").cast("double").isNull(),
            1
        ).otherwise(0)
    ).alias("NonNumeric")

).collect()[0]


ov_missing = int(
    observation_value_characteristics["Missing"]
)

ov_numeric = int(
    observation_value_characteristics["Numeric"]
)

ov_nonnumeric = int(
    observation_value_characteristics["NonNumeric"]
)

observation_characteristics_pdf = pd.DataFrame([
    {
        "Category": "Missing / Blank",
        "Count": ov_missing,
        "Percentage": round(
            ov_missing / total_rows * 100, 3
        )
    },
    {
        "Category": "Numeric",
        "Count": ov_numeric,
        "Percentage": round(
            ov_numeric / total_rows * 100, 3
        )
    },
    {
        "Category": "Non-Numeric",
        "Count": ov_nonnumeric,
        "Percentage": round(
            ov_nonnumeric / total_rows * 100, 3
        )
    }
])


# 8. NUMERIC OBSERVATION VALUE STATISTICS

numeric_values = (
    sensor_df
    .select(
        F.col("ObservationValue")
        .cast("double")
        .alias("value")
    )
    .filter(F.col("value").isNotNull())
)

numeric_stats = numeric_values.agg(
    F.count("value").alias("Count"),
    F.min("value").alias("Minimum"),
    F.max("value").alias("Maximum"),
    F.mean("value").alias("Mean"),
    F.stddev("value").alias("StdDev")
).collect()[0]


# Approximate quartiles using the FULL dataset
quantiles = numeric_values.approxQuantile(
    "value",
    [0.01, 0.25, 0.50, 0.75, 0.99],
    0.01
)

if len(quantiles) == 5:

    p01, q1, median, q3, p99 = quantiles

    iqr = q3 - q1

    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)

    # Count outliers across ALL numeric observations
    outlier_count = numeric_values.filter(
        (F.col("value") < lower_bound) |
        (F.col("value") > upper_bound)
    ).count()

else:

    p01 = q1 = median = q3 = p99 = None
    iqr = lower_bound = upper_bound = None
    outlier_count = None


numeric_stats_pdf = pd.DataFrame([{
    "Count": int(numeric_stats["Count"]),
    "Minimum": numeric_stats["Minimum"],
    "1st Percentile": p01,
    "Q1": q1,
    "Median": median,
    "Q3": q3,
    "99th Percentile": p99,
    "Maximum": numeric_stats["Maximum"],
    "Mean": numeric_stats["Mean"],
    "Std Dev": numeric_stats["StdDev"],
    "IQR": iqr,
    "Lower Outlier Bound": lower_bound,
    "Upper Outlier Bound": upper_bound,
    "IQR Outliers": outlier_count,
    "Outlier %": (
        outlier_count / int(numeric_stats["Count"]) * 100
        if outlier_count is not None and int(numeric_stats["Count"]) > 0
        else None
    )
}])

if "Outlier %" in numeric_stats_pdf.columns:
    numeric_stats_pdf["Outlier %"] = (
        numeric_stats_pdf["Outlier %"].round(3)
    )


# 9. MOST COMMON OBSERVATION VALUES

top_values_df = (
    sensor_df
    .groupBy(
        "ObservationType",
        "ObservationValue"
    )
    .count()
    .withColumn(
        "Percentage",
        F.col("count") / F.lit(total_rows) * 100
    )
    .orderBy(F.desc("count"))
    .limit(500)
)

top_values_pdf = top_values_df.toPandas()

top_values_pdf.columns = [
    "ObservationType",
    "ObservationValue",
    "Count",
    "Percentage"
]

top_values_pdf["Percentage"] = (
    top_values_pdf["Percentage"].round(3)
)


# 10. ATTRIBUTEMAP KEYS

attribute_keys_df = (
    sensor_df
    .select(
        F.explode(
            F.map_keys("AttributeMap")
        ).alias("AttributeKey")
    )
    .groupBy("AttributeKey")
    .count()
    .withColumn(
        "Percentage of Rows",
        F.col("count") / F.lit(total_rows) * 100
    )
    .orderBy(F.desc("count"))
)

attribute_keys_pdf = attribute_keys_df.toPandas()

attribute_keys_pdf.columns = [
    "AttributeKey",
    "Count",
    "Percentage of Rows"
]

attribute_keys_pdf["Percentage of Rows"] = (
    attribute_keys_pdf["Percentage of Rows"].round(3)
)


# 11. ATTRIBUTEMAP SIZE

attribute_map_stats = sensor_df.agg(

    F.sum(
        F.when(
            F.col("AttributeMap").isNull(),
            1
        ).otherwise(0)
    ).alias("Null Maps"),

    F.avg(
        F.when(
            F.col("AttributeMap").isNotNull(),
            F.size("AttributeMap")
        )
    ).alias("Average Keys"),

    F.max(
        F.when(
            F.col("AttributeMap").isNotNull(),
            F.size("AttributeMap")
        )
    ).alias("Maximum Keys")

).collect()[0]


attribute_map_pdf = pd.DataFrame([{
    "Null AttributeMaps":
        int(attribute_map_stats["Null Maps"]),

    "Average Keys per Map":
        attribute_map_stats["Average Keys"],

    "Maximum Keys":
        attribute_map_stats["Maximum Keys"]
}])


# 12. BUILD HTML

def table_html(df):

    return df.to_html(
        index=False,
        classes="profile-table",
        border=0,
        escape=True
    )


html_report = f"""
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<title>SensorData Full Profile Report</title>

<style>

body {{
    font-family: Arial, Helvetica, sans-serif;
    margin: 0;
    padding: 30px;
    background: #f5f6f7;
    color: #222;
}}

.container {{
    max-width: 1600px;
    margin: auto;
}}

h1 {{
    font-size: 32px;
}}

h2 {{
    margin-top: 45px;
    border-bottom: 2px solid #ddd;
    padding-bottom: 10px;
}}

.summary {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 15px;
}}

.card {{
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,.1);
}}

.card-number {{
    font-size: 25px;
    font-weight: bold;
}}

.profile-table {{
    width: 100%;
    border-collapse: collapse;
    background: white;
    margin-top: 15px;
}}

.profile-table th {{
    background: #333;
    color: white;
    padding: 10px;
    text-align: left;
    position: sticky;
    top: 0;
}}

.profile-table td {{
    padding: 9px;
    border-bottom: 1px solid #ddd;
}}

.profile-table tr:hover {{
    background: #f0f0f0;
}}

.warning {{
    background: #fff3cd;
    padding: 15px;
    border-radius: 6px;
    margin-top: 15px;
}}

</style>

</head>

<body>

<div class="container">

<h1>SensorData Full Profile Report</h1>

<div class="summary">

<div class="card">
<div>Total Rows</div>
<div class="card-number">{total_rows:,}</div>
</div>

<div class="card">
<div>Total Columns</div>
<div class="card-number">{len(sensor_df.columns)}</div>
</div>

<div class="card">
<div>Analysis</div>
<div class="card-number">Full Dataset</div>
</div>

</div>


<h2>1. Column Information</h2>

{table_html(column_pdf)}


<h2>2. Missing Values</h2>

{table_html(missing_pdf)}


<h2>3. Approximate Unique Values</h2>

{table_html(unique_pdf)}


<h2>4. EventDate</h2>

{table_html(date_pdf)}


<h2>5. ObservationType Distribution</h2>

{table_html(observation_type_pdf)}


<h2>6. ObservationValue Characteristics</h2>

<div class="warning">

ObservationValue is stored as a string in the source data.
The report therefore checks which values can be interpreted
as numeric and which remain non-numeric.

</div>

{table_html(observation_characteristics_pdf)}


<h2>7. Numeric ObservationValue Statistics</h2>

{table_html(numeric_stats_pdf)}


<h2>8. Most Common Observation Values</h2>

{table_html(top_values_pdf)}


<h2>9. AttributeMap Keys</h2>

{table_html(attribute_keys_pdf)}


<h2>10. AttributeMap Structure</h2>

{table_html(attribute_map_pdf)}


</div>

</body>

</html>
"""


# 13. SAVE HTML REPORT

PROFILE_DIR = "/Volumes/senseofsensors_catalog/confidential/profiles"
report_path = f"{PROFILE_DIR}/SensorData_Full_Profile.html"

dbutils.fs.put(
    report_path,
    html_report,
    overwrite=True
)

print("==============================================")
print("FULL SENSOR DATA PROFILE REPORT CREATED")
print("==============================================")
print(report_path)