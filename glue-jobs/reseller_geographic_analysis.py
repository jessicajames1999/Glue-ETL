"""
ETL Job: Reseller Geographic Analysis
Input: reseller_csv, region_csv
Output: reseller_geographic (CSV in S3)
Transformation: Join resellers with regions, add geographic groupings
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue Catalog
reseller_df = glueContext.create_dynamic_frame.from_catalog(
    database="adventureworks_db",
    table_name="reseller_csv"
).toDF()

region_df = glueContext.create_dynamic_frame.from_catalog(
    database="adventureworks_db",
    table_name="region_csv"
).toDF()

# Join resellers with their regions
# Note: Adjust join key based on actual column names
reseller_with_region = reseller_df.alias("r").join(
    region_df.alias("reg"),
    col("r.city") == col("reg.country"),  # Adjust this join condition
    "left"
)

# Add geographic grouping
reseller_geographic = reseller_with_region.withColumn(
    "region_group",
    when(col("country-region").isin("USA", "Canada"), "North America")
    .when(col("country-region").isin("UK", "Germany", "France"), "Europe")
    .otherwise("Other")
)

# Select relevant columns
final_output = reseller_geographic.select(
    "resellerkey",
    "reseller",
    "businesstype",
    "city",
    "state-province",
    "country-region",
    "region_group"
)

# Write to S3
output_path = "s3://adventureworksredshift/transformed/reseller_geographic/"

final_output.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Reseller geographic analysis written to {output_path}")

job.commit()