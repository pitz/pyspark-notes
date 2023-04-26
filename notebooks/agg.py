import pyspark.sql.functions as F 

# We have two forms to use `agg` functions in our queries

# .agg(F.count("id").alias("count"), 
#      F.count("without_conclusion").alias("count_without_conclusion"))

# or using maps:
# .agg(
#   {"id":   "count",
#    "without_conclusion": "count"}
# )
# I personally don't like this approach since we can't use an alias() for the aggregated columns.

runs = (
  spark.table("x.x")
       .where(F.col("finished_at").isNull())
       .withColumn("without_conclusion", F.when(F.col("finished_at").isNull(), 0).otherwise(1)) 
       .where(F.col("started_at") < "2023-04-16")
       .where(F.col("type") == "x")
       .select("tax_id", "id", "without_conclusion")
       .groupby("tax_id")
       .agg(
         {"id":   "count",
          "without_conclusion": "count"} # I don't like this approach
       )
       .display(10))


