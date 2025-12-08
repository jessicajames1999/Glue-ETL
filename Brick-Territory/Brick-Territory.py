"""
ETL Job 3: Brick to Territory Mapping
=====================================
Map geographic bricks to territories through sub-territory hierarchy:
Brick → Sub-Territory (L1) → Territory

INPUT:  dim_brick.csv, dim_sub_territory.csv, dim_territory.csv
OUTPUT: map_brick_to_territory (silver layer)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, upper, trim, when, lit, row_number, current_date, coalesce
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
# STEP 1: Load source data
# ============================================
print("=== Step 1: Loading source data ===")

brick = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_brick.csv")
print(f"Bricks: {brick.count()}")

sub_territory = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_sub_territory.csv")
print(f"Sub-territories: {sub_territory.count()}")

territory = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_territory.csv")
print(f"Territories: {territory.count()}")

# ============================================
# STEP 2: Build territory hierarchy lookup
# ============================================
print("\n=== Step 2: Building territory hierarchy ===")

# Join sub-territory with territory
hierarchy = sub_territory.alias("sub") \
    .join(
        territory.alias("terr"),
        col("sub.territory_id") == col("terr.territory_id"),
        "inner"
    ) \
    .select(
        col("sub.sub_territory_id"),
        col("sub.sub_territory_name"),
        col("sub.territory_id"),
        col("terr.territory_name"),
        col("terr.region"),
        col("terr.state")
    )

print(f"Hierarchy entries: {hierarchy.count()}")
hierarchy.show(5)

# ============================================
# STEP 3: Map bricks to sub-territories by state
# ============================================
print("\n=== Step 3: Mapping bricks to sub-territories ===")

# Match bricks to territories by state
brick_territory_join = brick.alias("b") \
    .join(
        hierarchy.alias("h"),
        upper(col("b.state")) == upper(col("h.state")),
        "left"
    )

# BUG INTRODUCED: Some bricks don't match due to state code mismatch
# We're not handling all state variations (some bricks have NULL territory)

# Assign each brick to one sub-territory (first match within state)
window = Window.partitionBy("b.brick_id").orderBy("h.sub_territory_id")

brick_mapped = brick_territory_join \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .select(
        col("b.brick_id"),
        col("b.brick_name"),
        col("b.state").alias("brick_state"),
        col("h.sub_territory_id"),
        col("h.territory_id"),
        col("h.territory_name"),
        col("h.region")
    )

matched_count = brick_mapped.filter(col("territory_id").isNotNull()).count()
unmatched_count = brick_mapped.filter(col("territory_id").isNull()).count()

print(f"Bricks matched to territory: {matched_count}")
print(f"Bricks with NO territory (UNASSIGNED): {unmatched_count}")

# ============================================
# STEP 4: Handle unassigned bricks
# ============================================
print("\n=== Step 4: Handling unassigned bricks ===")

# Mark unassigned bricks
map_brick_to_territory = brick_mapped \
    .withColumn("sub_territory_id", 
        coalesce(col("sub_territory_id"), lit(-1))) \
    .withColumn("territory_id", 
        coalesce(col("territory_id"), lit(-1))) \
    .withColumn("territory_name", 
        coalesce(col("territory_name"), lit("UNASSIGNED"))) \
    .withColumn("region", 
        coalesce(col("region"), lit("UNASSIGNED"))) \
    .withColumn("mapping_id", row_number().over(Window.orderBy("brick_id"))) \
    .withColumn("effective_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date"))

# Select final columns
final_columns = [
    "mapping_id", "brick_id", "sub_territory_id", "territory_id", 
    "territory_name", "region", "effective_date", "end_date"
]
map_brick_to_territory = map_brick_to_territory.select(final_columns)

print(f"\nTotal mappings: {map_brick_to_territory.count()}")
print("\nTerritory distribution:")
map_brick_to_territory.groupBy("territory_name").count().orderBy(col("count").desc()).show(10)

print("\nRegion distribution:")
map_brick_to_territory.groupBy("region").count().orderBy(col("count").desc()).show()

# ============================================
# STEP 5: Write output
# ============================================
print("\n=== Step 5: Writing output ===")

map_brick_to_territory.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/silver/map_brick_to_territory_csv/")

print(f"SUCCESS: Written to s3://{S3_BUCKET}/silver/map_brick_to_territory/")

# Log issues
unassigned = map_brick_to_territory.filter(col("territory_id") == -1).count()
if unassigned > 0:
    print(f"\nWARNING: {unassigned} bricks are UNASSIGNED to any territory!")
    print("These bricks will cause sales to show as 'Unassigned' region:")
    map_brick_to_territory.filter(col("territory_id") == -1).show(10)

job.commit()