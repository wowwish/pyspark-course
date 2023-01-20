# Figure out the most popular movie by counting the number of ratings by movie - other approach
# Here, we print the movie name along with the movie ID
# The "u.item" of the MovieLens dataset gives us the mapping for MovieID - Movie Name
# We could use a DataSet to map movie ID's to names, then join it with our ratings dataset - but this comes 
# with unneccessary overhead and is suitable only when our dataset has so many unique movie IDs to than what 
# can be fit in the memory of a single executor machine (which is not the case - not much data in our case).
# We could also just keep a dictionary loaded in this driver program by reading the "u.item" file, or we could 
# even let SPARK automatically forward it to each executor (worker node) when needed. But what if the table was massive ? 
# We'd only want to transfer it once to each executor (worker node) and keep it there (as a Broadcast variable).

# We can Broadcast objects to the executors (worker nodes), such that they are always there whenever needed, using sc.broadcast(obj) on SparkContext object.
# The object can be got by using obj.value(). The Boradcaster object can be used however you want (in map functions, UDFs, etc)

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs # For loading the u.item file which is an encoded text file

# custom function in python to generate the mapping dictionary to be broadcasted
def loadMovieNames():
    movieNames = {} # Initialize empty dictionary
    # READ u.ITEM FILE as an encoded text file using the python standard encoder from the "codecs" module
    with codecs.open("C:/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|') # split every line in the file by the pipe character
            movieNames[int(fields[0])] = fields[1] # build the mapping dictionary
    return movieNames

# Initialize SparkSession to work with DataFrames
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Use the SparkContext that is part of SparkSession to broadcast the dictionary mapping movie ID to name that is
# returned by the loadMovieNames() UDF
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

# Aggregate the movies by their ID and count the number of ratings for each movie
movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID] # using .value() on the Broadcast object to get the actual data object that was Broadcasted
    # Then we use the movie ID as key to get its name from the mapping dictionary that was retrieved from the Broadcast object
    # REMEMBER: This is only useful to share small data objects to every node of your cluster.

# SPARK's User Defined Function (UDF) created and registered using func.udf(). These are optimized functions.
lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
# We pass in the arguments of the "movieID" column into our UDF and create the new "movieTitle" column using the results
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results by descending order of movie ratings count
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
