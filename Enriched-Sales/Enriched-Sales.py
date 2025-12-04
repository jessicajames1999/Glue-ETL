"""
ETL Job 1: Enriched Sales
Joins fact_sales with all dimension tables
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

spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/fact_sales.csv").createOrReplaceTempView("fact_sales")

spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/dim_product.csv").createOrReplaceTempView("dim_product")

spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/dim_region.csv").createOrReplaceTempView("dim_region")

spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/dim_reseller.csv").createOrReplaceTempView("dim_reseller")

spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3://{S3_BUCKET}/dim_salesperson.csv").createOrReplaceTempView("dim_salesperson")

enriched_sales = spark.sql("""
    SELECT 
        f.SalesOrderNumber,
        f.OrderDate,
        f.Quantity,
        f.Sales,
        f.Cost,
        ROUND(f.Sales - f.Cost, 2) AS Profit,
        p.Product,
        p.Category AS ProductCategory,
        r.Region,
        r.Country,
        re.Reseller,
        s.Salesperson
    FROM fact_sales f
    LEFT JOIN dim_product p ON f.ProductKey = p.ProductKey
    LEFT JOIN dim_region r ON f.SalesTerritoryKey = r.SalesTerritoryKey + 1
    LEFT JOIN dim_reseller re ON f.ResellerKey = re.ResellerKey
    LEFT JOIN dim_salesperson s ON f.EmployeeKey = s.EmployeeKey
""")

enriched_sales.write.mode("overwrite").option("header", "true").csv(f"s3://{S3_BUCKET}/silver/enriched_sales_csv/")

print(f"Done! Wrote {enriched_sales.count()} rows")
job.commit()