# JOIN DATA FRAMES

# [LEFT].join([RIGHT], on=[PREDICATE] how=[METHOD])

# Joining the DataFrames on 'affinity-id'
# from pyspark.sql.functions import col
# joined_df = (
#     br__series_contract.underwriting_spendings
#     .join(
#         br__contract.jumanji_runs,
#         col('br__series_contract.underwriting_spendings.affinity-id') == col('br__contract.jumanji_runs.affinity-id'),
#         'inner'
#     )
# )

import pyspark.sql.functions as F 

applied_hardcuts = (
  spark.table("table_a")
       .where(F.col("hardcut_applied") == True)
       .where(F.col("product") == "credit-card")
       .where(F.col("hardcut_checked_at") > "2023-04-01")
       .alias("hardcuts"))

blocked_tax_id_runs = (
  spark.table("table_b")
       .where(F.col("run__affinity_type") == "xxx")
       .withColumnRenamed("run__affinity_id", "affinity_id")
       .drop(F.col("run__finished_at"))
       .alias("runs")
       .join(bv_cadastral_applied_hardcuts, F.col("hardcuts.underwriting__id") == F.col("runs.affinity_id"))
       .show(10))
  
# IF we need to filter more than one column, we could use this:
# .join(bv_cadastral_applied_hardcuts, (F.col("hardcuts.underwriting__id") == F.col("runs.affinity_id")) 
#    & (F.col("hardcuts.tax_id") == F.col("runs.tax_id")))
 

