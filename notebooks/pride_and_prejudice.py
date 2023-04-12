# IMPORTING MODULES
# from pyspark.sql.functions import split, explode, lower, regexp_extract
import pyspark.sql.functions as F

# IMPORTING DATA

# importing the book Pride and Prejudice on Databricks
file_location = "/FileStore/tables/1342_0.txt"
file_type = "txt"

# READING THE DATA

# .alias() provide a clean and explicit way to name your columns
# another valid way to do that, is using .withColumnRenamed()
book = spark.read.text(file_location)
lines = book.select(split(book.value, " ").alias("line"))

# EXPLODE

# explode() function when applied to a column containing a container-like data structure (such as an array)
# it will take each element and give it its own row: Explode the data from of array[String] into a data frame of String
words = lines.select(explode(col("line")).alias("word"))

# LOWER CASES

# lowering the words is quite simple, we only have to use the .lower() function
words_in_lower_case = words.select(lower(col("word")).alias("word_lower"))

# REMOVE SPECIAL CHARACTERS

# after having all the words in lower case, we want to remove special characters
# for that, we're going to use regexp_extract
words_clean = words_in_lower_case.select(
    regexp_extract(col("word_lower"), "[a-z]+", 0).alias("word"))
# words_clean.show(20)

# FILTERING ROWS

# after select()-ing records, filtering is probably the most frequent and easiest operation to perform on your data
# PySpark provides two identical methods to perform this task: where() and filter().
not_null_words = words_clean.filter(col("word") != "")
# not_null_words.show(20)

# it's also possible to use "not" in a expression, for that we have ~
# for instance, we could have `notnull_words = words_clean.filter(~ (col("word") == ""))

# if we think about performance, it would be better if we filter the data before the transformations
# since we don't have a big data frame, it's not a problem to do some manipulation before filtering it

# GROUPING THE WORDS

results = not_null_words.groupby(col("word")).count()
results_a = results.orderBy("count", ascending=False).show(10)

# OR
import pyspark.sql.functions as F

file_location = "/FileStore/tables/1342_0.txt"
file_type = "txt"

results = (
  spark.read.text(file_location)
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word_lower"))
    .select(F.regexp_extract(F.col("word_lower"), "[a-z]+", 0).alias("word"))
    .filter(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
    .orderBy("count", ascending = False)
    .show(10)
)
                                                
