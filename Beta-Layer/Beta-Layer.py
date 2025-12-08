"""
ETL Job 5: Beta Layer Validation
================================
Validate current month's prescription data (beta layer) before promoting to production:
1. Apply business rules (no negatives, valid dates, etc.)
2. Compare to 6-month historical average
3. Flag anomalies > 50% deviation
4. Promote clean data or reject with errors

INPUT:  fact_prescriptions_beta.csv, fact_prescriptions_enriched (from Job 4)
OUTPUT: fact_prescriptions_validated (gold layer), validation_report
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, lit, coalesce, abs as spark_abs,
    sum as spark_sum, count as spark_count, avg as spark_avg,
    round as spark_round, current_timestamp, concat_ws,
    month, year, to_date
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
# STEP 1: Load data
# ============================================
print("=== Step 1: Loading data ===")

# Beta layer (current month to validate)
beta = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/fact_prescriptions_beta.csv")
print(f"Beta records (current month): {beta.count()}")

# Historical data (6 months)
historical = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/fact_prescriptions.csv")
print(f"Historical records (6 months): {historical.count()}")

# Product mapping for enrichment
map_product = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/silver/map_product_master_csv/")

product_master = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/raw/dim_product_master.csv")

# ============================================
# STEP 2: Apply Business Rules
# ============================================
print("\n=== Step 2: Applying business rules ===")

beta_validated = beta \
    .withColumn("rule_1_quantity_positive", 
        when(col("quantity") > 0, "PASS").otherwise("FAIL: Negative quantity")) \
    .withColumn("rule_2_revenue_positive", 
        when(col("revenue") > 0, "PASS").otherwise("FAIL: Negative revenue")) \
    .withColumn("rule_3_valid_date", 
        when(col("rx_date").isNotNull(), "PASS").otherwise("FAIL: Missing date")) \
    .withColumn("rule_4_valid_hcp", 
        when(col("hcp_id").isNotNull(), "PASS").otherwise("FAIL: Missing HCP"))

# Combine all rule results
beta_validated = beta_validated \
    .withColumn("all_rules_passed",
        when(
            (col("rule_1_quantity_positive") == "PASS") &
            (col("rule_2_revenue_positive") == "PASS") &
            (col("rule_3_valid_date") == "PASS") &
            (col("rule_4_valid_hcp") == "PASS"),
            True
        ).otherwise(False)
    ) \
    .withColumn("validation_errors",
        when(col("all_rules_passed") == False,
            concat_ws("; ",
                when(col("rule_1_quantity_positive") != "PASS", col("rule_1_quantity_positive")),
                when(col("rule_2_revenue_positive") != "PASS", col("rule_2_revenue_positive")),
                when(col("rule_3_valid_date") != "PASS", col("rule_3_valid_date")),
                when(col("rule_4_valid_hcp") != "PASS", col("rule_4_valid_hcp"))
            )
        ).otherwise(lit(None))
    )

rules_failed = beta_validated.filter(col("all_rules_passed") == False).count()
print(f"Records failing business rules: {rules_failed}")

# ============================================
# STEP 3: Enrich beta with product info
# ============================================
print("\n=== Step 3: Enriching beta data ===")

beta_enriched = beta_validated.alias("b") \
    .join(
        map_product.alias("mp"),
        col("b.raw_product_id") == col("mp.raw_product_id"),
        "left"
    ) \
    .join(
        product_master.alias("pm"),
        col("mp.master_product_id") == col("pm.master_product_id"),
        "left"
    ) \
    .select(
        col("b.*"),
        col("pm.master_product_id"),
        col("pm.brand_name"),
        col("pm.therapeutic_class")
    )

# ============================================
# STEP 4: Calculate 6-month historical averages
# ============================================
print("\n=== Step 4: Calculating historical averages ===")

# Enrich historical with product info
historical_enriched = historical.alias("h") \
    .join(
        map_product.alias("mp"),
        col("h.raw_product_id") == col("mp.raw_product_id"),
        "left"
    ) \
    .join(
        product_master.alias("pm"),
        col("mp.master_product_id") == col("pm.master_product_id"),
        "left"
    ) \
    .select(
        col("h.*"),
        col("pm.master_product_id"),
        col("pm.brand_name")
    )

# Calculate monthly averages by product
historical_monthly = historical_enriched \
    .withColumn("rx_month", month(to_date(col("rx_date")))) \
    .groupBy("master_product_id", "brand_name") \
    .agg(
        spark_count("*").alias("hist_total_records"),
        spark_round(spark_sum("revenue"), 2).alias("hist_total_revenue"),
        spark_round(spark_avg("revenue"), 2).alias("hist_avg_revenue_per_rx")
    ) \
    .withColumn("hist_monthly_avg_revenue", 
        spark_round(col("hist_total_revenue") / 6, 2))  # 6 months

print("Historical averages by product:")
historical_monthly.orderBy(col("hist_total_revenue").desc()).show(10)

# ============================================
# STEP 5: Calculate beta totals and compare
# ============================================
print("\n=== Step 5: Comparing beta to historical ===")

# Aggregate beta by product
beta_summary = beta_enriched \
    .groupBy("master_product_id", "brand_name") \
    .agg(
        spark_count("*").alias("beta_records"),
        spark_round(spark_sum("revenue"), 2).alias("beta_revenue")
    )

# Join with historical averages
comparison = beta_summary.alias("beta") \
    .join(
        historical_monthly.alias("hist"),
        col("beta.master_product_id") == col("hist.master_product_id"),
        "left"
    ) \
    .select(
        col("beta.master_product_id"),
        col("beta.brand_name"),
        col("beta.beta_records"),
        col("beta.beta_revenue"),
        col("hist.hist_monthly_avg_revenue"),
        col("hist.hist_total_revenue")
    )

# Calculate deviation
ANOMALY_THRESHOLD = 0.5  # 50% deviation

comparison_with_deviation = comparison \
    .withColumn("deviation_pct",
        when(col("hist_monthly_avg_revenue") > 0,
            spark_round(
                (col("beta_revenue") - col("hist_monthly_avg_revenue")) / col("hist_monthly_avg_revenue") * 100, 
                2
            )
        ).otherwise(lit(None))
    ) \
    .withColumn("is_anomaly",
        when(spark_abs(col("deviation_pct")) > (ANOMALY_THRESHOLD * 100), True)
        .otherwise(False)
    ) \
    .withColumn("anomaly_type",
        when(col("deviation_pct") > (ANOMALY_THRESHOLD * 100), "SPIKE")
        .when(col("deviation_pct") < -(ANOMALY_THRESHOLD * 100), "DROP")
        .otherwise("NORMAL")
    )

print("\n=== ANOMALY DETECTION RESULTS ===")
print(f"Threshold: {ANOMALY_THRESHOLD * 100}% deviation from 6-month average\n")

anomalies = comparison_with_deviation.filter(col("is_anomaly") == True)
anomaly_count = anomalies.count()
print(f"ANOMALIES DETECTED: {anomaly_count}")

if anomaly_count > 0:
    print("\n*** PRODUCTS WITH ANOMALIES ***")
    anomalies.select(
        "brand_name", "beta_revenue", "hist_monthly_avg_revenue", 
        "deviation_pct", "anomaly_type"
    ).orderBy(spark_abs(col("deviation_pct")).desc()).show(20, truncate=False)

# Show all products comparison
print("\nAll products comparison (sorted by deviation):")
comparison_with_deviation.select(
    "brand_name", "beta_revenue", "hist_monthly_avg_revenue", 
    "deviation_pct", "anomaly_type"
).orderBy(spark_abs(col("deviation_pct")).desc()).show(20, truncate=False)

# ============================================
# STEP 6: Generate validation report
# ============================================
print("\n=== Step 6: Generating validation report ===")

validation_report = comparison_with_deviation \
    .withColumn("validation_timestamp", current_timestamp()) \
    .withColumn("validation_status",
        when(col("is_anomaly") == True, "REQUIRES_REVIEW")
        .otherwise("PASSED")
    )

# Summary stats
total_products = validation_report.count()
passed = validation_report.filter(col("validation_status") == "PASSED").count()
review = validation_report.filter(col("validation_status") == "REQUIRES_REVIEW").count()

print(f"\nVALIDATION SUMMARY:")
print(f"  Total products: {total_products}")
print(f"  Passed: {passed}")
print(f"  Requires Review: {review}")

# ============================================
# STEP 7: Update beta records with validation status
# ============================================
print("\n=== Step 7: Updating beta records ===")

# Join anomaly flags back to beta records
beta_final = beta_enriched.alias("b") \
    .join(
        comparison_with_deviation.select(
            "master_product_id", "is_anomaly", "anomaly_type", "deviation_pct"
        ).alias("c"),
        col("b.master_product_id") == col("c.master_product_id"),
        "left"
    ) \
    .withColumn("validation_status",
        when(col("all_rules_passed") == False, "FAILED")
        .when(col("is_anomaly") == True, "ANOMALY_REVIEW")
        .otherwise("PASSED")
    ) \
    .select(
        col("b.rx_id"),
        col("b.rx_date"),
        col("b.hcp_id"),
        col("b.hco_id"),
        col("b.raw_product_id"),
        col("b.quantity"),
        col("b.revenue"),
        col("b.source_system"),
        col("b.master_product_id"),
        col("b.brand_name"),
        col("b.therapeutic_class"),
        col("validation_status"),
        col("b.validation_errors"),
        col("c.anomaly_type"),
        col("c.deviation_pct")
    )

# Summary
print("\nBeta records by validation status:")
beta_final.groupBy("validation_status").count().show()

# ============================================
# STEP 8: Write outputs
# ============================================
print("\n=== Step 8: Writing outputs ===")

# Write validated beta records


beta_final.write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"s3://{S3_BUCKET}/gold/fact_prescriptions_validated_csv/")

print(f"Written: s3://{S3_BUCKET}/gold/fact_prescriptions_validated/")

# Write validation report
validation_report.write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"s3://{S3_BUCKET}/gold/validation_report_csv/")

print(f"Written: s3://{S3_BUCKET}/gold/validation_report_csv/")

# ============================================
# FINAL SUMMARY
# ============================================
print("\n" + "="*60)
print("BETA LAYER VALIDATION COMPLETE")
print("="*60)

if anomaly_count > 0:
    print(f"\n⚠️  WARNING: {anomaly_count} products have revenue anomalies!")
    print("   Review the validation_report before promoting to production.")
    print("\n   Top anomalies to investigate:")
    anomalies.select("brand_name", "beta_revenue", "hist_monthly_avg_revenue", "deviation_pct") \
        .orderBy(spark_abs(col("deviation_pct")).desc()) \
        .show(5, truncate=False)
else:
    print("\n✅ All products within normal range. Safe to promote to production.")

job.commit()