from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

sales_path = "/Volumes/workspace/lakeflow_examples/bakery/sales"
dp.create_streaming_table("bakery_sales_bronze")

@dp.append_flow(target = "bakery_sales_bronze")
def bakery_sales_ingest_flow():
  return (
      spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("header", "true")
          .load(f"{sales_path}")
  )

customers_path = "/Volumes/workspace/lakeflow_examples/bakery/customers"
dp.create_streaming_table("bakery_customers_bronze")

@dp.append_flow(target = "bakery_customers_bronze")
def bakery_customers_ingest_flow():
  return (
      spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "true")
          .option("header", "true")
          .load(f"{customers_path}")
  )