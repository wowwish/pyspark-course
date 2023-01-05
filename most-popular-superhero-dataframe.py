# Here, we build a super hero social network. We use the information of superheroes who appear in the same
# comic book, to build this network. The "Marvel-graph.txt" file contains IDs of superheroes who appear within
# the same comic book (one book per per line, a hero may come up as the first ID in multiple lines! ) and the 
# "Marvel-names.txt" file contains the ID to name mapping for super heroes.

# We will split off the hero ID in the begining of each line (because this ID belongs to the hero who we are
# currently concentrating on in the network). Next, we will count the number of space seperated numbers in the line
# and subtract one from it to get the total number of connections. We group by the hero ID's to add up connections
# split into multiple lines for hero IDs coming first in multiple lines. Then, we sort the final DataFrame by total 
# connections and use a lookup DataSet to get the name of the most popular hero ID.

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
    
# We sort the result DataFrame in descending order of the "connections" column and get a single Row object DataFrame which
# corresponds to the most popular hero.
mostPopular = connections.sort(func.col("connections").desc()).first()

# Here, we query the lookup DataFrame with the id of the the most popular hero and get a single Row DataFrame with
# just the name of the most popular hero as the only Row object
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

# printing result
print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

# Stop the SparkSession
spark.stop()
