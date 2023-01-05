# BoilerPlate stuff
# Import SparkSession to work with SPARK DataFrame objects
from pyspark.sql import SparkSession
from pyspark.sql import Row # Imported to work with Row objects of DataFrame
from pyspark.sql import functions as func # Imported for additinal built-in functions on DataFrame objects
# Importing SparkSession for DataFrame usage
# Setup the SparkSession - not using SparkContext here, using only SPARK SQL and no RDDs (which require SparkContext)
# Create a SparkSession to expose the SPARK SQL interface using SparkSession.builder. Use appName() to set the app name for SPARK Web UI
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

# Reading the CSV file directly using SparkSession - specifying that the input file contains a header row.
# The inferSchema option tells SPARK to figure out the Schema for the DataFrame from the file contents.
# Then we user the .csv() method to parse the input CSV file.
lines = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")

# Select only age and numFriends columns - throw away data that you don't need for your problem as early as possible
# This saves resources in our cluster by not carrying around unwanted data!
friendsByAge = lines.select("age", "friends")

# From friendsByAge, we group by "age" field and then compute average of "friends" for each group of "age"
friendsByAge.groupBy("age").avg("friends").show()

# Sorted the result DataFrame by "age" field
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Pretty printing - Format the average number of friends for each age group (Rounding to 2 decimal places)
# Take the average of the "friends" field and round it to 2 decimal places - do this as an operation on the
# aggregated set of results/records from groupBy().
# REMEMBER: explicitly mention func.round()/func.avg() to explicitly call the spark DataFrame methods and prevent
# collision with python's implementation of the same functions
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# Pretty printing - Format the average number of friends for each age group and change the column name of the field
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

# Stop the SparkSession
spark.stop()