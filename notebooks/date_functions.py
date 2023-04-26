import pyspark.sql.functions as F

cce_runs = (
    spark.table("br__contract.jumanji__runs")
    .where(F.col("run__finished_at").isNotNull())
    .where(F.col("run__started_at") > "2023-04-19")
    .where(F.col("run__started_at") < "2023-04-24")
    .where(F.col("run__affinity_type") == "contract_credit_card_eligibility__underwrite_definitions")
    .withColumn("total", F.datediff(F.col("run__finished_at"), F.col("run__started_at")))
    .withColumn("total_in_s", (F.col("run__finished_at").cast("long") - F.col("run__started_at").cast("long")))
)

cce_runs.select(F.col("run__started_at"),
                F.col("run__finished_at"),
                F.col("run__id"),
                F.col("total"),
                F.col("total_in_s")).display(10)

# Using PySpark functions to create new columns
# to work with Date and TimeStamp
# https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/
# https://sparkbyexamples.com/pyspark/pyspark-timestamp-difference-seconds-minutes-hours/
