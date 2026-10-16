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

# CELL ********************

df = spark.sql("""
SELECT *
FROM LH_AgenticWorkflow.dbo.publicholidays
WHERE countryOrRegion = 'United States'
  AND date >= '2024-01-01'
  AND date < '2025-01-01'
""")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Distinct holiday names and dates in Poland for 2024
pl_2024 = spark.sql("""
SELECT DISTINCT holidayName, CAST(date AS DATE) AS holidayDate
FROM LH_AgenticWorkflow.dbo.publicholidays
WHERE countryOrRegion = 'Poland'
  AND date >= '2024-01-01'
  AND date < '2025-01-01'
ORDER BY holidayDate, holidayName
""")

print(f"Poland - public holidays in 2024 ({pl_2024.count()} distinct entries):")
for row in pl_2024.collect():
    print(f"{row['holidayDate']}  {row['holidayName']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Poland - number of public holidays per year
pl_by_year = spark.sql("""
SELECT
    YEAR(date) AS holidayYear,
    COUNT(DISTINCT holidayName, CAST(date AS DATE)) AS holidayCount
FROM LH_AgenticWorkflow.dbo.publicholidays
WHERE countryOrRegion = 'Poland'
GROUP BY YEAR(date)
ORDER BY holidayYear
""")

display(pl_by_year)

# test2


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
