# WORKING WIHT COLUMNS

blocked_tax_id_runs = (
  spark.table("br__contract.jumanji__runs")
       .where(F.col("run__tax_id") == "yy")
       .where(F.col("run__affinity_type") == "xx")
       .withColumn("without_conclusion", F.when(F.col("run__finished_at").isNull(), 0).otherwise(1))
       .withColumnRenamed("run__affinity_id", "affinity_id")
       .drop(F.col("run__finished_at"))
       .select("run__flow").summary() 
       .display(20))

# RENAMING COLUMNS
# Do you want to rename a column?
# You can do that using .alias() or .withColumnRenamed("run__affinity_id", "affinity_id"). I personally prefer using .withColumnRenamed("run__affinity_id", "affinity_id")

# REMOVING COLUMNS
# Do you want to remove a column?
# You can use .drop() to remove columns instead of selecting all columns that you want with the .select()

# DESCRIBE (count, mean, stddev, min, max)
# .describe() will perform some calculations and delivery these metrics for you: count, mean, stddev, min, max

# SUMMARY 
# Almost the same as .describe() but with more flexibility and some percentiles.
# Also, you can define with metrics you want to see: .select("run__flow").summary("min", "10%", "90%") 

# Seems that .describe() and .summary() can present different results, depending on PySpark version. Nice, isn't?








