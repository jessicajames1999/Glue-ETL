"""
ETL Job 1: Product Mastering
============================
Match messy raw product names to master product catalog using:
1. Exact NDC match
2. Fuzzy name matching (Levenshtein)
3. Rules-based matching (parse strength + generic name)

INPUT:  dim_product_raw.csv, dim_product_master.csv
OUTPUT: map_product_master (silver layer)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, coalesce, 
    levenshtein, length, least, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "awsgilead"

# ============================================
# STEP 1: Load source data
# ============================================
print("=== Step 1: Loading source data ===")

product_raw = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_product_raw.csv")
print(f"Raw products: {product_raw.count()}")

product_master = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_product_master.csv")
print(f"Master products: {product_master.count()}")

# ============================================
# STEP 2: Standardize NDC codes for matching
# ============================================
print("\n=== Step 2: Standardizing NDC codes ===")

product_raw_clean = product_raw \
    .withColumn("ndc_clean", regexp_replace(col("ndc_code"), "-", "")) \
    .withColumn("name_upper", upper(trim(col("source_product_name")))) \
    .withColumn("strength_clean", upper(regexp_replace(col("strength"), " ", "")))

product_master_clean = product_master \
    .withColumn("ndc_clean", regexp_replace(col("standard_ndc"), "-", "")) \
    .withColumn("brand_upper", upper(trim(col("brand_name")))) \
    .withColumn("generic_upper", upper(trim(col("generic_name")))) \
    .withColumn("strength_clean", upper(regexp_replace(col("standard_strength"), " ", "")))

# ============================================
# STEP 3: Exact NDC Match
# ============================================
print("\n=== Step 3: Exact NDC matching ===")

exact_matches = product_raw_clean.alias("raw") \
    .join(
        product_master_clean.alias("master"),
        col("raw.ndc_clean") == col("master.ndc_clean"),
        "inner"
    ) \
    .select(
        col("raw.raw_product_id"),
        col("master.master_product_id"),
        lit("EXACT_NDC").alias("match_type"),
        lit(1.0).alias("confidence_score"),
        lit("ndc_code").alias("matched_on")
    )

exact_count = exact_matches.count()
print(f"Exact NDC matches: {exact_count}")

matched_ids = exact_matches.select("raw_product_id")
unmatched_raw = product_raw_clean.join(matched_ids, on="raw_product_id", how="left_anti")
print(f"Remaining unmatched: {unmatched_raw.count()}")

# ============================================
# STEP 4: Fuzzy Name Matching
# ============================================
print("\n=== Step 4: Fuzzy name matching ===")

fuzzy_candidates = unmatched_raw.alias("raw").crossJoin(product_master_clean.alias("master"))

fuzzy_scored = fuzzy_candidates \
    .withColumn("lev_distance", levenshtein(col("raw.name_upper"), col("master.brand_upper"))) \
    .withColumn("max_len", least(length(col("raw.name_upper")), length(col("master.brand_upper")))) \
    .withColumn("similarity", 
        when(col("max_len") > 0, 1 - (col("lev_distance") / col("max_len")))
        .otherwise(0)
    )

# FIX: Increased threshold from 0.3 to 0.7 to prevent low-confidence false matches
FUZZY_THRESHOLD = 0.7

fuzzy_matches = fuzzy_scored \
    .filter(col("similarity") >= FUZZY_THRESHOLD) \
    .select(
        col("raw.raw_product_id"),
        col("master.master_product_id"),
        lit("FUZZY_NAME").alias("match_type"),
        col("similarity").alias("confidence_score"),
        lit("product_name").alias("matched_on")
    )

window = Window.partitionBy("raw_product_id").orderBy(col("confidence_score").desc())

fuzzy_best = fuzzy_matches \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank")

fuzzy_count = fuzzy_best.count()
print(f"Fuzzy matches: {fuzzy_count}")

# ============================================
# STEP 5: Rules-based matching for remaining
# ============================================
print("\n=== Step 5: Rules-based matching ===")

fuzzy_matched_ids = fuzzy_best.select("raw_product_id")
still_unmatched = unmatched_raw.join(fuzzy_matched_ids, on="raw_product_id", how="left_anti")

rules_candidates = still_unmatched.alias("raw").crossJoin(product_master_clean.alias("master"))

rules_matches = rules_candidates \
    .filter(col("raw.strength_clean") == col("master.strength_clean")) \
    .filter(
        col("raw.name_upper").contains(col("master.brand_upper")) | 
        col("raw.name_upper").contains(col("master.generic_upper"))
    ) \
    .select(
        col("raw.raw_product_id"),
        col("master.master_product_id"),
        lit("RULES_BASED").alias("match_type"),
        lit(0.85).alias("confidence_score"),
        lit("strength+name").alias("matched_on")
    )

rules_best = rules_matches \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank")

rules_count = rules_best.count()
print(f"Rules-based matches: {rules_count}")

# ============================================
# STEP 6: Combine all matches
# ============================================
print("\n=== Step 6: Combining all matches ===")

all_matches = exact_matches \
    .union(fuzzy_best) \
    .union(rules_best) \
    .withColumn("mapping_id", row_number().over(Window.orderBy("raw_product_id")))

final_columns = ["mapping_id", "raw_product_id", "master_product_id", "match_type", "confidence_score", "matched_on"]
map_product_master = all_matches.select(final_columns)

print(f"\nTotal mappings: {map_product_master.count()}")
print("\nMatch type distribution:")
map_product_master.groupBy("match_type").count().show()

# ============================================
# STEP 7: Write output
# ============================================
print("\n=== Step 7: Writing output ===")

map_product_master.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/silver/map_product_master_csv/")

print(f"SUCCESS: Written to s3://{S3_BUCKET}/silver/map_product_master_csv/")

low_confidence = map_product_master.filter(col("confidence_score") < 0.7).count()
print(f"\nWARNING: {low_confidence} mappings with confidence < 0.7")

job.commit()
