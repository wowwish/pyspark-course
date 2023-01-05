# HANDLING UNSTRUCTURED DATA WITH SQL - NOT A GOOD FIT !!
# DATAFRAMES ARE REALLY MADE TO BE USED WITH STRUCTURED DATA - THIS IS A PLACE WHERE USING AN RDD WILL BE BETTER SUITED
# YOU CAN LOAD DATA AS RDD AND CONVERT IT TO A DATAFRAME FOR FURTHER PROCESSING - OR VICE VERSA
# SOME USEFUL TRICKS:
# The additional functions from pyspark.sql.functions can be very useful
# func.explode() - similar to flatmap of RDDs; explodes values of columns into rows - Returns a new row for each element in the given array or map
# func.split() - find individual words within a line of text by splitting it with a delimiter
# func.lower() - convert string to lowercase
# Passing columns as parameters:
#   * func.split(inputDF.value, "\\W+") - here were refer to the "value" column/field of the inputDF DataFrame
#   * filter(wordsDF.word != "") - here we refer to the "word" column/field of the wordsDF dataframe
#   * Can also do func.col("columnName") to refer to a column


# Importing SparkSession for DataFrame usage
from pyspark.sql import SparkSession
# Imported for using additional built-in Spark DataFrame methods
from pyspark.sql import functions as func

# Setup the SparkSession - not using SparkContext here, using only SPARK SQL and no RDDs (which require SparkContext)
# Create a SparkSession to expose the SPARK SQL interface using SparkSession.builder. Use appName() to set the app name for SPARK Web UI
# getOrCreate() allows us to connect back into persistent SparkSessions already present, or create a new SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of book.txt into a dataframe - our initial DataFrame will just have Row objects with a column/field
# called "value" (default column/field name when the input data has no structure) for each line of text
inputDF = spark.read.text("file:///SparkCourse/book.txt")

# Split each Row using a regular expression that extracts words by using non-word delimiters
# We use func.explode() to put individual words of the func.split() operation into multiple rows
# We then rename the single column of words using .alias()
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase and save as new DataFrame
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word and save as new DataFrame
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts and save as new DataFrame
wordCountsSorted = wordCounts.sort("count")

# Show the results - here, we show the full set of results instead of just the first 20 results.
wordCountsSorted.show(wordCountsSorted.count())

# Stop the SparkSession
spark.stop()
