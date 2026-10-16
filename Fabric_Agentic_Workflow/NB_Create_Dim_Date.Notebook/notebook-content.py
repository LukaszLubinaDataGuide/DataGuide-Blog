# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d92fa7a4-9836-40ba-918c-9446d5278216",
# META       "default_lakehouse_name": "LH_AgenticWorkflow",
# META       "default_lakehouse_workspace_id": "50be2564-b12a-419d-93e8-fce57529a5b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "d92fa7a4-9836-40ba-918c-9446d5278216"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # dim_date — date dimension with US public holidays
# 
# Builds `dbo.dim_date` in **LH_AgenticWorkflow** from a generated date spine and enriches every
# day with a public-holiday flag and holiday name taken from the existing `dbo.publicholidays`
# table (Azure Open Datasets sample), filtered to **United States** holidays only.
# 
# **Source:** `dbo.publicholidays` — `countryOrRegion`, `holidayName`, `normalizeHolidayName`,
# `isPaidTimeOff`, `countryRegionCode`, `date`
# **Target:** `dbo.dim_date` (Delta, full overwrite — the dimension is deterministic and cheap to rebuild)

# PARAMETERS CELL ********************

# Parameters — override from a pipeline / notebookutils.notebook.run()
START_DATE = "2015-01-01"          # first day of the dimension (inclusive)
END_DATE   = "2035-12-31"          # last day of the dimension (inclusive)

SOURCE_HOLIDAYS_TABLE = "dbo.publicholidays"
TARGET_TABLE          = "dbo.dim_date"

COUNTRY_REGION_CODE = "US"                 # ISO code in publicholidays.countryRegionCode
COUNTRY_OR_REGION   = "United States"      # fallback: countryRegionCode is NULL for some rows
WEEK_START_MONDAY   = True                 # day_of_week 1 = Monday when True, 1 = Sunday when False

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

print(f"Default lakehouse : {spark.conf.get('trident.lakehouse.name', 'n/a')}")
print(f"Building          : {TARGET_TABLE}  [{START_DATE} .. {END_DATE}]")
print(f"Holiday source    : {SOURCE_HOLIDAYS_TABLE}  (country = {COUNTRY_OR_REGION!r} / {COUNTRY_REGION_CODE!r})")

# CELL ********************

# 1. Date spine — one row per calendar day, no gaps
dates_df = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS date"
    )
)

print(f"Date spine rows: {dates_df.count():,}")

# CELL ********************

# 2. Calendar attributes
#    day_of_week follows WEEK_START_MONDAY: ISO (Mon=1) or US (Sun=1).
# Spark >= 3.0 rejects the week-based pattern letters ("u", "Y"), so ISO day-of-week
# is derived from dayofweek() (Sun=1) and the ISO year from EXTRACT(YEAROFWEEK).
iso_dow = ((F.dayofweek("date") + 5) % 7) + 1             # Mon=1 .. Sun=7
us_dow  = F.dayofweek("date")                             # Sun=1 .. Sat=7

calendar_df = (
    dates_df
    .withColumn("date_key",            F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",                F.year("date"))
    .withColumn("quarter",             F.quarter("date"))
    .withColumn("month",               F.month("date"))
    .withColumn("month_name",          F.date_format("date", "MMMM"))
    .withColumn("month_short_name",    F.date_format("date", "MMM"))
    .withColumn("day_of_month",        F.dayofmonth("date"))
    .withColumn("day_of_year",         F.dayofyear("date"))
    .withColumn("day_of_week",         iso_dow if WEEK_START_MONDAY else us_dow)
    .withColumn("day_name",            F.date_format("date", "EEEE"))
    .withColumn("day_short_name",      F.date_format("date", "EEE"))
    .withColumn("iso_week",            F.weekofyear("date"))
    .withColumn("iso_year",            F.expr("extract(YEAROFWEEK FROM date)").cast("int"))
    .withColumn("year_month",          F.date_format("date", "yyyyMM").cast("int"))
    .withColumn("year_month_name",     F.date_format("date", "yyyy-MM"))
    .withColumn("year_quarter_name",   F.concat_ws("-Q", F.year("date"), F.quarter("date")))
    .withColumn("first_day_of_month",  F.trunc("date", "month"))
    .withColumn("last_day_of_month",   F.last_day("date"))
    .withColumn("first_day_of_quarter", F.trunc("date", "quarter"))
    .withColumn("first_day_of_year",   F.trunc("date", "year"))
    .withColumn("is_weekend",          iso_dow.isin(6, 7))
    .withColumn("is_last_day_of_month", F.col("date") == F.last_day("date"))
)

display(calendar_df.limit(5))

# CELL ********************

# 3. US public holidays — one row per date.
#    publicholidays can hold several entries for the same day (e.g. an observed
#    variant), so names are de-duplicated and collapsed into a single string.
holidays_raw = (
    spark.table(SOURCE_HOLIDAYS_TABLE)
    .where(
        (F.col("countryRegionCode") == COUNTRY_REGION_CODE)
        | (F.col("countryOrRegion") == COUNTRY_OR_REGION)
    )
    .select(
        F.to_date("date").alias("date"),
        F.col("holidayName").alias("holiday_name"),
        F.col("normalizeHolidayName").alias("holiday_name_normalized"),
        F.col("isPaidTimeOff").alias("is_paid_time_off"),
    )
    .where(F.col("date").between(F.lit(START_DATE).cast("date"), F.lit(END_DATE).cast("date")))
)

holidays_df = (
    holidays_raw
    .groupBy("date")
    .agg(
        F.array_join(F.array_sort(F.collect_set("holiday_name")), " | ").alias("holiday_name"),
        F.array_join(F.array_sort(F.collect_set("holiday_name_normalized")), " | ").alias("holiday_name_normalized"),
        F.max(F.coalesce(F.col("is_paid_time_off"), F.lit(False))).alias("is_paid_time_off"),
        F.size(F.collect_set("holiday_name")).alias("holiday_count"),
    )
)

print(f"Distinct US holiday dates in range: {holidays_df.count():,}")
display(holidays_df.orderBy("date").limit(20))

# CELL ********************

# 4. Join holidays onto the calendar and derive the business-day flag
dim_date_df = (
    calendar_df.join(holidays_df, on="date", how="left")
    .withColumn("is_public_holiday", F.col("holiday_name").isNotNull())
    .withColumn("holiday_name", F.col("holiday_name"))
    .withColumn("is_paid_time_off", F.coalesce(F.col("is_paid_time_off"), F.lit(False)))
    .withColumn("holiday_count", F.coalesce(F.col("holiday_count"), F.lit(0)))
    .withColumn("is_business_day", ~F.col("is_weekend") & F.col("holiday_name").isNull())
    .select(
        "date_key", "date",
        "year", "quarter", "month", "month_name", "month_short_name",
        "day_of_month", "day_of_year", "day_of_week", "day_name", "day_short_name",
        "iso_week", "iso_year", "year_month", "year_month_name", "year_quarter_name",
        "first_day_of_month", "last_day_of_month", "first_day_of_quarter", "first_day_of_year",
        "is_weekend", "is_last_day_of_month",
        "is_public_holiday", "holiday_name", "holiday_name_normalized",
        "is_paid_time_off", "holiday_count",
        "is_business_day",
    )
    .orderBy("date_key")
)

display(dim_date_df.where(F.col("is_public_holiday")).limit(20))

# CELL ********************

# 5. Write the dimension (full rebuild)
(
    dim_date_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

spark.sql(f"OPTIMIZE {TARGET_TABLE}")
print(f"Written: {TARGET_TABLE}")

# CELL ********************

# 6. Validation — row count, gap check, holidays per year
checks = spark.sql(f"""
    SELECT
        COUNT(*)                                            AS row_count,
        COUNT(DISTINCT date)                                AS distinct_dates,
        DATEDIFF(MAX(date), MIN(date)) + 1                  AS expected_days,
        MIN(date)                                           AS min_date,
        MAX(date)                                           AS max_date,
        SUM(CAST(is_public_holiday AS INT))                 AS holiday_days,
        SUM(CAST(is_business_day   AS INT))                 AS business_days
    FROM {TARGET_TABLE}
""").collect()[0]

assert checks["row_count"] == checks["distinct_dates"] == checks["expected_days"], \
    f"Date spine is not contiguous/unique: {checks}"
print(checks)

display(spark.sql(f"""
    SELECT year, COUNT(*) AS holiday_days,
           array_join(array_sort(collect_set(holiday_name)), ', ') AS holidays
    FROM {TARGET_TABLE}
    WHERE is_public_holiday
    GROUP BY year
    ORDER BY year
"""))
