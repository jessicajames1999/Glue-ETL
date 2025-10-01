"""
ETL Job: Sales Summary by Product
Input: sales_csv
Output: sales_summary_by_product (CSV in S3)
Transformation: Aggregate total sales, quantity, and order count by product
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum, count, avg

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read sales data from Glue Catalog
sales_df = glueContext.create_dynamic_frame.from_catalog(
    database="adventureworks_db",
    table_name="sales_csv"
).toDF()

# Transformation: Aggregate by product
sales_summary = sales_df.groupBy("productkey").agg(
    sum("sales").alias("total_sales"),
    sum("quantity").alias("total_quantity"),
    count("salesordernumber").alias("order_count"),
    avg("unitprice").alias("avg_unit_price")
)

# Write to S3 as CSV
output_path = "s3://adventureworksredshift/transformed/sales_summary_by_product/"

sales_summary.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Sales summary written to {output_path}")

job.commit()