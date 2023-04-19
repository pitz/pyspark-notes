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

# Reads the data from the table using the spark.table() function.
# Filters the data to include only those rows where the value in the run__tax_id column is equal to "yy" and 
# the value in the run__affinity_type column is equal to "xx".
# Adds a new column to the DataFrame named "without_conclusion" using the withColumn() function. 
# This column is computed using the when() and otherwise() functions from the pyspark.sql.functions module. 
# It sets the value to 0 if the value in the run__finished_at column is null and 1 otherwise.
# Renames the column run__affinity_id to affinity_id using the withColumnRenamed() function.
# Drops the run__finished_at column using the drop() function.
# Selects the run__flow column from the DataFrame and computes summary statistics using the summary() function.
# Displays the first 20 rows of the resulting summary DataFrame using the display() function.

# Overall, this query is used to filter, transform, and summarize data from a PySpark DataFrame in 
# order to obtain a summary of blocked tax ID runs that have not been concluded. The summary includes 
# various statistical measures such as count, mean, and standard deviation of the run__flow column.
