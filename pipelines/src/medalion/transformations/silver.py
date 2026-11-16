from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


dp.create_streaming_table("bakery_customers_silver")

dp.create_auto_cdc_flow(
  target = "bakery_customers_silver",
  source = "bakery_customers_bronze",
  keys = ["customerID"],
  sequence_by = col("customerID"),
  except_column_list = ["_rescued_data", "_source_file", ""],
  stored_as_scd_type = 2
)