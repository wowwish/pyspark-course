# Here, we find the most obscure super heroes (super heroes that have only one or the smallest number of 
# hero connections in comic books).
# REFER THE most-popular-superhero-dataframe.py SCRIPT FOR MORE INFORMATION ON THE INPUT DATA FILES AND 
# MORE EXPLANATION OF THE CODE HERE.

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Create a lookup DatSet/DataFrame to map hero ID to name by reading the space seperated text file "Marvel-names.txt"
names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

# Reading the input text file as a DataFrame with a single column named "value" (Default column name)
lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# Trim leading/trailing whitespaces from each line of the file saved in the "values" column of the single
# column DataFrame and then split each element by space and get the first element of the returned array. 
# This first element will be saved within a new column called "id". Another new column "connections" is 
# created to contain the value of the number connections (count of IDs other than the first ID in each line).
# This connection count value is calculated by getting the size of the array returned by func.split() and subtracting one 
# from the array size for each element in the "value" column. These connection counts are finally aggregated based on the 
# newly created "id" column and the connection counts are totaled for each unique movie ID. The new aggregated
# DatFrame now has a "id" column and an aggregated "connections" column with the total count of connections for
# that specific ID.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Get the value of the least number of connections in the DataFrame by aggregating the entire DataFrame's "connections" column
# without groups, and then using func.min and grabbing the only Row object returned and getting the Row object's first value
minConnectionCount = connections.agg(func.min("connections")).first()[0]

# Create DataFrame with all records having the the minimum number of connections that was found above
minConnections = connections.filter(func.min("connections") == minConnectionCount)

# Join the DataFrame of hero IDs with minimum connections to their corresponding hero names using the commmon
# "id" column between both the DataFrames using .join() (SQL inner join by Default)
minConnectionsWithNames = minConnections.join(names, "id") # Always make sure to use joins on the smallest 
# DataFrame possible by whittling down useless information from the DataFrame

# Dislay all the records of heroes with the least number of connections
print("The following characters have only " + str(minConnectionCount) + " connection(s):")
minConnectionsWithNames.select("name").show()

# Stop the SparkSession
spark.stop()
