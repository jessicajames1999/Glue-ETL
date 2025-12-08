"""
ETL Job 4: Sales Enrichment
===========================
Join prescription facts with all dimension and mapping tables to create
a fully enriched sales table for analytics.

INPUT:  fact_prescriptions.csv, all mapping tables, all dimension tables
OUTPUT: fact_prescriptions_enriched (gold layer)

JOINS:
- fact_prescriptions → map_product_master → dim_product_master (product info)
- fact_prescriptions → dim_hcp → map_hcp_to_brick → map_brick_to_territory (geography)
- fact_prescriptions → dim_hco (healthcare org)
- fact_prescriptions → map_brick_to_territory → dim_sales_rep (rep assignment)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, lit, coalesce, sum as spark_sum, count as spark_count,
    round as spark_round, current_timestamp, row_number
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "awsgilead"  # UPDATE THIS

# ============================================
# STEP 1: Load all source data
# ============================================
print("=== Step 1: Loading source data ===")

# Fact table
prescriptions = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/fact_prescriptions.csv")
print(f"Prescriptions: {prescriptions.count()}")

# Mapping tables (from previous jobs)
map_product = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/silver/map_product_master_csv/")
print(f"Product mappings: {map_product.count()}")

map_hcp_brick = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/silver/map_hcp_to_brick_csv/")
print(f"HCP-Brick mappings: {map_hcp_brick.count()}")

map_brick_terr = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/silver/map_brick_to_territory_csv/")
print(f"Brick-Territory mappings: {map_brick_terr.count()}")

# Dimension tables
product_master = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_product_master.csv")
print(f"Product master: {product_master.count()}")

hcp = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_hcp.csv")
print(f"HCPs: {hcp.count()}")

hco = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_hco.csv")
print(f"HCOs: {hco.count()}")

sales_rep = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_sales_rep.csv")
print(f"Sales reps: {sales_rep.count()}")

territory = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_territory.csv")
print(f"Territories: {territory.count()}")

# ============================================
# STEP 2: Enrich with Product Information
# ============================================
print("\n=== Step 2: Joining product information ===")

# Join prescriptions → map_product → product_master
rx_with_product = prescriptions.alias("rx") \
    .join(
        map_product.alias("mp"),
        col("rx.raw_product_id") == col("mp.raw_product_id"),
        "left"
    ) \
    .join(
        product_master.alias("pm"),
        col("mp.master_product_id") == col("pm.master_product_id"),
        "left"
    ) \
    .select(
        col("rx.*"),
        col("pm.master_product_id"),
        col("pm.brand_name"),
        col("pm.generic_name"),
        col("pm.therapeutic_class"),
        col("pm.standard_strength"),
        col("mp.match_type").alias("product_match_type"),
        col("mp.confidence_score").alias("product_confidence")
    )

print(f"After product join: {rx_with_product.count()}")

# Check for unmapped products
unmapped_products = rx_with_product.filter(col("master_product_id").isNull()).count()
print(f"Prescriptions with unmapped product: {unmapped_products}")

# ============================================
# STEP 3: Enrich with HCP Information
# ============================================
print("\n=== Step 3: Joining HCP information ===")

rx_with_hcp = rx_with_product.alias("rx") \
    .join(
        hcp.alias("hcp"),
        col("rx.hcp_id") == col("hcp.hcp_id"),
        "left"
    ) \
    .select(
        col("rx.*"),
        col("hcp.first_name").alias("hcp_first_name"),
        col("hcp.last_name").alias("hcp_last_name"),
        col("hcp.specialty").alias("hcp_specialty"),
        col("hcp.city").alias("hcp_city"),
        col("hcp.state").alias("hcp_state")
    )

# ============================================
# STEP 4: Enrich with Geography (Brick → Territory)
# ============================================
print("\n=== Step 4: Joining geography ===")

# Join HCP → Brick mapping
rx_with_brick = rx_with_hcp.alias("rx") \
    .join(
        map_hcp_brick.alias("hb"),
        col("rx.hcp_id") == col("hb.hcp_id"),
        "left"
    ) \
    .select(
        col("rx.*"),
        col("hb.brick_id"),
        col("hb.match_type").alias("brick_match_type"),
        col("hb.confidence_score").alias("brick_confidence")
    )

# Join Brick → Territory mapping
# BUG INTRODUCED: Using wrong join key causes duplicate rows for some records
rx_with_territory = rx_with_brick.alias("rx") \
    .join(
        map_brick_terr.alias("bt"),
        col("rx.brick_id") == col("bt.brick_id"),
        "left"
    ) \
    .join(
        territory.alias("t"),
        col("bt.territory_id") == col("t.territory_id"),
        "left"
    ) \
    .select(
        col("rx.*"),
        col("bt.territory_id"),
        col("t.territory_name"),
        col("t.region"),
        col("bt.sub_territory_id")
    )

print(f"After geography join: {rx_with_territory.count()}")

# ============================================
# STEP 5: Enrich with HCO Information
# ============================================
print("\n=== Step 5: Joining HCO information ===")

rx_with_hco = rx_with_territory.alias("rx") \
    .join(
        hco.alias("hco"),
        col("rx.hco_id") == col("hco.hco_id"),
        "left"
    ) \
    .select(
        col("rx.*"),
        col("hco.hco_name"),
        col("hco.hco_type")
    )

# ============================================
# STEP 6: Assign Sales Rep
# ============================================
print("\n=== Step 6: Assigning sales rep ===")

rx_with_rep = rx_with_hco.alias("rx") \
    .join(
        sales_rep.alias("sr"),
        col("rx.territory_id") == col("sr.territory_id"),
        "left"
    ) \
    .select(
        col("rx.*"),
        col("sr.rep_id"),
        col("sr.rep_name")
    )

# Handle multiple reps per territory - keep first one
window = Window.partitionBy("rx_id").orderBy("rep_id")
rx_deduped = rx_with_rep \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank")

print(f"After sales rep join: {rx_deduped.count()}")

# ============================================
# STEP 7: Final transformations
# ============================================
print("\n=== Step 7: Final transformations ===")

fact_enriched = rx_deduped \
    .withColumn("territory_name", 
        coalesce(col("territory_name"), lit("UNASSIGNED"))) \
    .withColumn("region", 
        coalesce(col("region"), lit("UNASSIGNED"))) \
    .withColumn("brand_name", 
        coalesce(col("brand_name"), lit("UNMAPPED"))) \
    .withColumn("etl_timestamp", current_timestamp())

# Select final columns
final_columns = [
    "rx_id", "rx_date", "hcp_id", "hco_id", "raw_product_id",
    "quantity", "revenue", "source_system",
    "master_product_id", "brand_name", "generic_name", "therapeutic_class", "standard_strength",
    "product_match_type", "product_confidence",
    "hcp_first_name", "hcp_last_name", "hcp_specialty", "hcp_city", "hcp_state",
    "brick_id", "brick_match_type", "brick_confidence",
    "territory_id", "territory_name", "region", "sub_territory_id",
    "hco_name", "hco_type",
    "rep_id", "rep_name",
    "etl_timestamp"
]

fact_prescriptions_enriched = fact_enriched.select(final_columns)

print(f"\nFinal enriched records: {fact_prescriptions_enriched.count()}")

# ============================================
# STEP 8: Data Quality Summary
# ============================================
print("\n=== Step 8: Data Quality Summary ===")

print("\nRecords by region:")
fact_prescriptions_enriched.groupBy("region") \
    .agg(
        spark_count("*").alias("record_count"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .show(15)

print("\nRecords by therapeutic class:")
fact_prescriptions_enriched.groupBy("therapeutic_class") \
    .agg(
        spark_count("*").alias("record_count"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .show(10)

# Check for issues
unassigned_region = fact_prescriptions_enriched.filter(col("region") == "UNASSIGNED").count()
unmapped_product = fact_prescriptions_enriched.filter(col("brand_name") == "UNMAPPED").count()
null_rep = fact_prescriptions_enriched.filter(col("rep_id").isNull()).count()

print(f"\nData Quality Issues:")
print(f"  - UNASSIGNED region: {unassigned_region} records")
print(f"  - UNMAPPED product: {unmapped_product} records")
print(f"  - No sales rep assigned: {null_rep} records")

# ============================================
# STEP 9: Write output
# ============================================
print("\n=== Step 9: Writing output ===")


fact_prescriptions_enriched.write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"s3://{S3_BUCKET}/gold/fact_prescriptions_enriched_csv/")

print(f"SUCCESS: Written to s3://{S3_BUCKET}/gold/fact_prescriptions_enriched/")

job.commit()