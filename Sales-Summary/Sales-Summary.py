"""
ETL Job 3: Sales Summary
Aggregate sales by region, salesperson, and product category
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

# Read from Job 1 output
spark.read.parquet(f"s3://{S3_BUCKET}/silver/enriched_sales/") \
    .createOrReplaceTempView("enriched_sales")

# Sales by Region
sales_by_region = spark.sql("""
    SELECT 
        Region,
        Country,
        COUNT(*) AS total_orders,
        ROUND(SUM(Sales), 2) AS total_sales,
        ROUND(SUM(Profit), 2) AS total_profit
    FROM enriched_sales
    GROUP BY Region, Country
    ORDER BY total_sales DESC
""")

sales_by_region.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/gold/sales_by_region/")
sales_by_region.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/gold/sales_by_region_csv/")

# Sales by Salesperson
sales_by_rep = spark.sql("""
    SELECT 
        Salesperson,
        COUNT(*) AS total_orders,
        ROUND(SUM(Sales), 2) AS total_sales,
        ROUND(SUM(Profit), 2) AS total_profit
    FROM enriched_sales
    GROUP BY Salesperson
    ORDER BY total_sales DESC
""")

sales_by_rep.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/gold/sales_by_rep/")
sales_by_rep.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/gold/sales_by_rep_csv/")

# Sales by Product Category
sales_by_category = spark.sql("""
    SELECT 
        ProductCategory,
        COUNT(*) AS total_orders,
        ROUND(SUM(Sales), 2) AS total_sales,
        ROUND(SUM(Profit), 2) AS total_profit
    FROM enriched_sales
    GROUP BY ProductCategory
    ORDER BY total_sales DESC
""")

sales_by_category.write.mode("overwrite").parquet(f"s3://{S3_BUCKET}/gold/sales_by_category/")
sales_by_category.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/gold/sales_by_category_csv/")

print("Done! Created all summary tables")
job.commit()