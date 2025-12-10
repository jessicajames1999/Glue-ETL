"""
ETL Job 2: HCP to Brick Mapping
===============================
Assign each Healthcare Physician (HCP) to a geographic brick using:
1. Exact ZIP code match
2. Fuzzy city name matching
3. State-level fallback

INPUT:  dim_hcp.csv, dim_brick.csv
OUTPUT: map_hcp_to_brick (silver layer)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, upper, trim, split, explode, when, lit, 
    levenshtein, length, row_number, current_date, array_contains
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

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

hcp = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_hcp.csv")
print(f"HCPs: {hcp.count()}")

brick = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_brick.csv")
print(f"Bricks: {brick.count()}")

# ============================================
# STEP 2: Prepare data for matching
# ============================================
print("\n=== Step 2: Preparing data ===")

# Standardize HCP data
hcp_clean = hcp \
    .withColumn("zip_clean", trim(col("zip"))) \
    .withColumn("city_upper", upper(trim(col("city")))) \
    .withColumn("state_upper", upper(trim(col("state"))))

# Explode brick zip codes into individual rows for matching
brick_exploded = brick \
    .withColumn("zip_array", split(col("zip_codes"), ",")) \
    .withColumn("single_zip", explode(col("zip_array"))) \
    .withColumn("single_zip", trim(col("single_zip"))) \
    .withColumn("city_upper", upper(trim(col("city")))) \
    .withColumn("state_upper", upper(trim(col("state"))))

print(f"Brick-zip combinations: {brick_exploded.count()}")

# ============================================
# STEP 3: Exact ZIP Match
# ============================================
print("\n=== Step 3: Exact ZIP matching ===")

zip_matches = hcp_clean.alias("hcp") \
    .join(
        brick_exploded.alias("brick"),
        col("hcp.zip_clean") == col("brick.single_zip"),
        "inner"
    ) \
    .select(
        col("hcp.hcp_id"),
        col("brick.brick_id"),
        lit("EXACT_ZIP").alias("match_type"),
        lit(1.0).alias("confidence_score")
    ) \
    .dropDuplicates(["hcp_id"])

zip_count = zip_matches.count()
print(f"Exact ZIP matches: {zip_count}")

# Get unmatched HCPs
matched_hcp_ids = zip_matches.select("hcp_id")
unmatched_hcp = hcp_clean.join(matched_hcp_ids, on="hcp_id", how="left_anti")
print(f"Remaining unmatched: {unmatched_hcp.count()}")

# ============================================
# STEP 4: Fuzzy City + State Match
# ============================================
print("\n=== Step 4: Fuzzy city matching ===")

# Get unique brick cities (not exploded)
brick_cities = brick \
    .withColumn("city_upper", upper(trim(col("city")))) \
    .withColumn("state_upper", upper(trim(col("state")))) \
    .select("brick_id", "city_upper", "state_upper") \
    .dropDuplicates()

# Cross join for fuzzy matching (only for unmatched HCPs)
fuzzy_candidates = unmatched_hcp.alias("hcp") \
    .join(
        brick_cities.alias("brick"),
        col("hcp.state_upper") == col("brick.state_upper"),  # Same state required
        "inner"
    )

# Calculate city name similarity
fuzzy_scored = fuzzy_candidates \
    .withColumn("lev_distance", levenshtein(col("hcp.city_upper"), col("brick.city_upper"))) \
    .withColumn("max_len", length(col("hcp.city_upper"))) \
    .withColumn("similarity", 
        when(col("max_len") > 0, 1 - (col("lev_distance") / col("max_len")))
        .otherwise(0)
    ) \
    .filter(col("similarity") >= 0.8)

# Keep best match per HCP
window = Window.partitionBy("hcp_id").orderBy(col("similarity").desc())

fuzzy_matches = fuzzy_scored \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .select(
        col("hcp.hcp_id"),
        col("brick.brick_id"),
        lit("FUZZY_CITY").alias("match_type"),
        col("similarity").alias("confidence_score")
    )

fuzzy_count = fuzzy_matches.count()
print(f"Fuzzy city matches: {fuzzy_count}")

# ============================================
# STEP 5: State-level fallback (assign to first brick in matching state)
# ============================================
print("\n=== Step 5: State fallback matching ===")

fuzzy_matched_ids = fuzzy_matches.select("hcp_id")
still_unmatched = unmatched_hcp.join(fuzzy_matched_ids, on="hcp_id", how="left_anti")

# FIX: Always match HCPs to bricks in the same state
# Previously, every 100th HCP was incorrectly matched to any state due to a bug
state_fallback = still_unmatched.alias("hcp") \
    .join(
        brick_cities.alias("brick"),
        col("hcp.state_upper") == col("brick.state_upper"),  # FIXED: Always enforce state matching
        "inner"
    ) \
    .withColumn("rank", row_number().over(Window.partitionBy("hcp.hcp_id").orderBy("brick.brick_id"))) \
    .filter(col("rank") == 1) \
    .select(
        col("hcp.hcp_id"),
        col("brick.brick_id"),
        lit("STATE_FALLBACK").alias("match_type"),
        lit(0.5).alias("confidence_score")
    )

fallback_count = state_fallback.count()
print(f"State fallback matches: {fallback_count}")

# ============================================
# STEP 6: Mark remaining as UNASSIGNED
# ============================================
print("\n=== Step 6: Marking unassigned HCPs ===")

all_matched_ids = zip_matches.select("hcp_id") \
    .union(fuzzy_matches.select("hcp_id")) \
    .union(state_fallback.select("hcp_id"))

final_unmatched = hcp_clean.join(all_matched_ids, on="hcp_id", how="left_anti")

unassigned = final_unmatched.select(
    col("hcp_id"),
    lit(-1).alias("brick_id"),  # -1 indicates unassigned
    lit("UNASSIGNED").alias("match_type"),
    lit(0.0).alias("confidence_score")
)

unassigned_count = unassigned.count()
print(f"Unassigned HCPs: {unassigned_count}")

# ============================================
# STEP 7: Combine all mappings
# ============================================
print("\n=== Step 7: Combining all mappings ===")

all_mappings = zip_matches \
    .union(fuzzy_matches) \
    .union(state_fallback) \
    .union(unassigned)

# Add metadata columns
map_hcp_to_brick = all_mappings \
    .withColumn("mapping_id", row_number().over(Window.orderBy("hcp_id"))) \
    .withColumn("effective_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date"))

final_columns = ["mapping_id", "hcp_id", "brick_id", "match_type", "confidence_score", "effective_date", "end_date"]
map_hcp_to_brick = map_hcp_to_brick.select(final_columns)

print(f"\nTotal mappings: {map_hcp_to_brick.count()}")
print("\nMatch type distribution:")
map_hcp_to_brick.groupBy("match_type").count().show()

# ============================================
# STEP 8: Write output
# ============================================
print("\n=== Step 8: Writing output ===")

map_hcp_to_brick.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/silver/map_hcp_to_brick_csv/")

print(f"SUCCESS: Written to s3://{S3_BUCKET}/silver/map_hcp_to_brick/")

# Log issues
low_confidence = map_hcp_to_brick.filter(col("confidence_score") < 0.7).count()
print(f"\nWARNING: {low_confidence} mappings with confidence < 0.7")

job.commit()
