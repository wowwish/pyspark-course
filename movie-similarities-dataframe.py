# ITEM-BASED COLLABORATIVE FILTERING (RECOMMENDER SYSTEM ALGORITHM - TO FIND SIMILAR MOVIES FROM THE MOVIELENS DATASET)
# We will use caching of DataFrames here, to improve performance!

# STEPS:
#   * Find every pair of movies that were watched by the same person
#   * Measure the similarity of their ratings across all users who watched both
#   * Sort by movie, then by similarity strength
#   * REMEMBER: THIS IS JUST ONE APPROACH TO DO THIS!

# MAKING IT A SPARK PROBLEM:
#   * Select UserID, movieID and rating columns
#   * Find every movie pair rated by the same user
#      - this can be done with a "self-join" operation
#      - at this point we have movie1, movie2, rating1 and rating2
#      - Filter out duplicate pairs: because (movie1, movie2) is same as (movie2, movie1)
#   * Compute "cosine similarity" scores for every pair
#      - Add x^2, y^2, xy columns
#      - Group by (movie1, movir2) pairs
#      - Compute similarity score for each aggregated pair


# CACHING DATAFRAMES/DATASETS:
#   * In this example, we will query the final dataset of movie similarities a couple of times
#   * Any time you will perform mote than one action on a DataFrame, you should cache it!
#      - Otherwise, SPARK might re-evaluate the the entier DataFrame all over again!
#   * Use .cache() method or .persist() method to do this
#      - What's the difference between .cache() and .persist() ?  
#      - .persist() optionally allows you to cache the DataFrame to disk instead of just memory, just in case a 
#         node fails. You can use .persist() to store the DataFrame in disk if you are okay with additional overhead
#         in the execution time. If you are oprimizing for execution speed, use .cache() to save the DataFrame in memory.


# Boilerplate stuff for importing SparkSession, build-in DataFrame functions and custome Schema definition
# WE USE DATAFRAMES HERE TO OPTIMIZE FOR THE MASSIVE SELF-JOIN OPERATION THAT WE WILL PERFORM
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys # imported for reading in data from the command-line

def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    # Cosine Similarity: sum(x_i*y_i) / (sqrt(sum(x_i*x_i)) * sqrt(sum(y_i*y_i))) for i = 1 to n
    # The movie data are viewed as vectors in an inner product space, and the cosine similarity is defined as the 
    # cosine of the angle between them, that is, the dot product of the vectors divided by the product of their lengths.
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        # Count the number of occurrences of this movie pair (It is indirectly the number of users who have rated
        # both the movies of this movie pair) for prioritization in future. The numPairs acts as a confidence score
        # for our actual cosineSimilarity score.
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    # func.when() is used here to check for cases where denominator is zero and the score is set to 0 in such cases
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]

# Use every available CPU core on the local system to execute this job (for the computationally intensive self-join operation)
# This is a bad thing to do when deploying this driver script in a Cluster!!
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

# Schema for 'u.item' input file
movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])

# Schema for 'u.data' input file
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("file:///SparkCourse/ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("file:///SparkCourse/ml-100k/u.data")

# Only keep relevant fields in the DataFrame to save up on Resources
ratings = movies.select("userId", "movieId", "rating")

# Emit every possible pair of movies rated by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
# self-join based on ratings by the same userID for different movies and also account for duplicate pairs
# as (movieID1, movieID2) is the same as (movieID2, movieID1). (movieIDs should not be same, we use ratings1.movieID
#  < ratings2.movieID condition here to make sure of it here.)
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        # Rename and select the columns that we want to use!
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


# Caching the computer cosine similarity of the movie pairs so that this DataFrame can be shared across executors (worker nodes)
moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    # Thresholds for filtering movie pairs - only display movie pairs with similarity score more than 0.97 and
    # with more than 50 users having reviewed both the movies of the pair
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1]) # Reading movieID from command-line argument

    # Filter for movies with this user provided movieID, that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
        


# COMMAND TO RUN: spark-submit .\movie-similarities-dataframe.py 50
# NOTE THAT THE OUTPUT PRINTING TOOK SOME TIME BECAUSE, FOR EACH MOVIE PAIR, WE WERE DOING A LOOKUP FROM THE
# 'movies' DATAFRAME USING THE 'getMovieName' FUNCTION THAT WE DEFINED, IN ORDER TO GET THE ACTUAL MOVIE NAME
# FOR THE MOVIE IDs IN THE PAIR.

# IMPROVE THE RESULTS - SOME IDEAS TO TRY:
#   * Discard bad ratings - only recommend good movies
#   * Try different similarity metrics (Pearson Correlation Coefficient, Jaccard Coefficient, Conditional Probability)
#   * Adjust the thresholds for minimum co-raters or minimum scores
#   * Invent a new similarity metric that takes the number of co-raters into account
#   * Use genre information in 'u.items' to boost scores from movies in the same genre