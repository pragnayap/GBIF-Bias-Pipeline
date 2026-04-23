# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table("gbif.bronze_occurrences")

df_silver = df \
    .filter(F.col("decimalLatitude").isNotNull()) \
    .filter(F.col("decimalLongitude").isNotNull()) \
    .filter(F.col("species").isNotNull()) \
    .filter(F.col("year").isNotNull()) \
    .filter(F.col("year").between(1900, 2026)) \
    .filter(F.col("month").between(1, 12)) \
    .withColumn("lat_bin",
        (F.floor(F.col("decimalLatitude")  / 5) * 5).cast("int")) \
    .withColumn("lon_bin",
        (F.floor(F.col("decimalLongitude") / 5) * 5).cast("int")) \
    .withColumn("grid_cell",
        F.concat_ws("_",
            F.col("lat_bin").cast("string"),
            F.col("lon_bin").cast("string"))) \
    .withColumn("country_clean",
        F.when(F.col("countryCode").isNull(), "UNKNOWN")
         .otherwise(F.upper(F.trim(F.col("countryCode"))))) \
    .withColumn("hemisphere",
        F.when(F.col("decimalLatitude") >= 0, "Northern")
         .otherwise("Southern")) \
    .withColumn("observer_key",
        F.when(F.col("recordedBy").isNull(), "anonymous")
         .otherwise(F.lower(F.trim(F.col("recordedBy"))))) \
    .dropDuplicates(["gbifID"])

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gbif.silver_occurrences")

print(f"✓ Silver table written: {df_silver.count():,} clean rows")

# COMMAND ----------

spark.sql("""
    SELECT
        CASE
            WHEN lat_bin BETWEEN  15 AND 75  AND lon_bin BETWEEN -170 AND  -50 THEN 'North America'
            WHEN lat_bin BETWEEN -60 AND 15  AND lon_bin BETWEEN  -85 AND  -30 THEN 'South America'
            WHEN lat_bin BETWEEN  35 AND 75  AND lon_bin BETWEEN  -15 AND   45 THEN 'Europe'
            WHEN lat_bin BETWEEN -40 AND 40  AND lon_bin BETWEEN  -20 AND   55 THEN 'Africa'
            WHEN lat_bin BETWEEN   0 AND 80  AND lon_bin BETWEEN   25 AND  180 THEN 'Asia'
            WHEN lat_bin BETWEEN -55 AND  -5 AND lon_bin BETWEEN  110 AND  180 THEN 'Oceania'
            ELSE 'Other'
        END AS continent,
        COUNT(*)                  AS observations,
        COUNT(DISTINCT species)   AS unique_species,
        COUNT(DISTINCT grid_cell) AS grid_cells
    FROM gbif.silver_occurrences
    GROUP BY 1
    ORDER BY 2 DESC
""").show()
