from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def employees():
  return spark.readStream.table("employees_cdf")

dp.create_streaming_table("employees_history_scd1")
dp.create_streaming_table("employees_history_scd2")

dp.create_auto_cdc_flow(
  target = "employees_history_scd2",
  source = "employees",
  keys = ["employee_id"],
  sequence_by = col("sequence_num"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "sequence_num"],
  stored_as_scd_type = "2"
)

dp.create_auto_cdc_flow(
  target = "employees_history_scd1",
  source = "employees",
  keys = ["employee_id"],
  sequence_by = col("sequence_num"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "sequence_num"],
  stored_as_scd_type = "1"
)