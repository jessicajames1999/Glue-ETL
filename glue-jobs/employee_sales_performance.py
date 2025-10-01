"""
ETL Job: Employee Sales Performance
Input: salesperson_csv, sales_csv, targets_csv
Output: employee_performance (CSV in S3)
Transformation: Calculate actual vs target sales by employee
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum, col, round

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data
salesperson_df = glueContext.create_dynamic_frame.from_catalog(
    database="adventureworks_db",
    table_name="salesperson_csv"
).toDF()

sales_df = glueContext.create_dynamic_frame.from_catalog(
    database="adventureworks_db",
    table_name="sales_csv"
).toDF()

targets_df = glueContext.create_dynamic_frame.from_catalog(
    database="adventureworks_db",
    table_name="targets_csv"
).toDF()

# Aggregate actual sales by employee
actual_sales = sales_df.groupBy("employeekey").agg(
    sum("sales").alias("actual_sales"),
    sum("cost").alias("actual_cost")
)

# Join with salesperson info
sales_with_employee = actual_sales.join(
    salesperson_df,
    "employeekey",
    "left"
)

# Join with targets
performance = sales_with_employee.join(
    targets_df,
    "employeekey",
    "left"
)

# Calculate performance metrics
final_performance = performance.withColumn(
    "target_achievement_pct",
    round((col("actual_sales") / col("target")) * 100, 2)
).withColumn(
    "variance",
    col("actual_sales") - col("target")
)

# Select final columns
output = final_performance.select(
    "employeekey",
    "employeeid",
    "salesperson",
    "title",
    "actual_sales",
    "actual_cost",
    "target",
    "target_achievement_pct",
    "variance"
)

# Write to S3
output_path = "s3://adventureworksredshift/transformed/employee_performance/"

output.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Employee performance written to {output_path}")

job.commit()