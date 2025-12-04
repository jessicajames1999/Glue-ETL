"""
ETL Job 2: Clean Customer Orders
Fix nulls, standardize countries, add customer segments
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "adventureworks-demo-gilead"

# Read CSV
spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/customer_orders.csv").createOrReplaceTempView("customer_orders")

# SQL: Clean and transform
cleaned_orders = spark.sql("""
    SELECT 
        order_id,
        customer_id,
        COALESCE(category, 'Unknown') AS category,
        order_amount,
        status,
        CASE country
            WHEN 'US' THEN 'United States'
            WHEN 'UK' THEN 'United Kingdom'
            WHEN 'FR' THEN 'France'
            WHEN 'CA' THEN 'Canada'
            WHEN 'DE' THEN 'Germany'
            ELSE country
        END AS country,
        order_date,
        CASE 
            WHEN order_amount >= 300 THEN 'High Value'
            WHEN order_amount >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM customer_orders
""")

# Write outputs
cleaned_orders.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/silver/cleaned_customer_orders/")
cleaned_orders.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/silver/cleaned_customer_orders_csv/")

print(f"Done! Wrote {cleaned_orders.count()} rows")
job.commit()