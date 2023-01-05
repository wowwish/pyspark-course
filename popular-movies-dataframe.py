# Figure out the most popular movie by counting the number of ratings by movie

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
# imports for defining the schema structure of input file
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe using custom schema (the tab seperated file is read using tab as delimiter)
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
# we order the movies by the descending order of the aggregated "count" column that we create using count()
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
