# SPARK.ML CAPABILITIES (limited only by the provided by SPARK.ML out of the box - SPARK.ML does not support neural networks):
#   * Feature extraction
#       - term frequency / Inverse Document Frequency for search
#   * Basic Statistics
#       - Chi squared test, Pearson or Spearman Correlation, min, max, mean, variance
#   * Linear Regression, Logistic Regression
#   * Support Vector Machines
#   * Naive Bayes Classifier
#   * Decision Trees
#   * K-Means Clustering
#   * Principal Component Analysis, Singular Value Decomposition
#   * Recommendations using Alternating Least Squares

# The previous SPARK machine learning library MLLib used RDDs and it is deprecated. The newer "SPARK.ML" library
# just used SPARK DataFrames for everything and this offers more interoperability with other SPARK APIs like SPARK SQL.


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS # Importing the ALS recommendation model
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    # Reading the data from input file using custom Schema structure that we built. We are not distributing
    # this lookup dictionary across the executors using broadcasting because we will only be using this once
    # , when we get the movie recommendations from the model, for looking up the movie names of the corresponding 
    # movie IDs. Hence, storing this lookup table as a dctionary accessible through only the the master script 
    # saves us resources. Furthermore, this data is small enough to be stored in the memory of master node.
    # DONT DISTRIBUTE DATA THAT YOU DONT HAVE TO
    with codecs.open("C:/SparkCourse/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Initiate the SparkSession
spark = SparkSession.builder.appName("ALSExample").getOrCreate()

# Set Up custom schema for the input data
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
names = loadMovieNames()

# Reading input data as distributed DataFrame
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("file:///SparkCourse/ml-100k/u.data")
    
print("Training recommendation model...")

# Using the Aleterating Least Squares (ALS) recommendation algorithm model that comes out of the box with Spark ML
# Setting up the ALS model - setting up the model hyperparameters: maximum iterations and regularization 
# We also set the column names of the user, item and rating for the input DataFrame that the model is going to
# be used on.
als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")

# Fitting and Training the model to the input DataFrame. Here, we are training the model with the entire input
# DataSet, but you can also split the input DataFrame into training and testing DataFrames
model = als.fit(ratings)

# Manually construct a dataframe of the user ID's we want reccommendationss for
userID = int(sys.argv[1]) # Integer userIDs taken from the command-line will be converted to dataframe for recommendation generation
userSchema = StructType([StructField("userID", IntegerType(), True)]) # Schema of DataFrame for Recommendation Generation contains only a single column of name 'userID'
users = spark.createDataFrame([[userID,]], userSchema) # [[userID,]] is used to trick SPARK into creating a DataFrame from a list of 
# Row objects ([userID,]) where our first and only Row object has only one column value 'userID'.

recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(userID))

for userRecs in recommendations:
    myRecs = userRecs[1]  # userRecs is a tuple of (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in myRecs: # myRecs is just the column of reccommendationss for the user (list of Row objects)
        movie = rec[0] # For each reccommendation in the list, extract the movie ID and rating
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))


# Stop the SparkSession
spark.stop()

# THINGS TO KEEP IN MIND:
#   * The ML algorithm performance is very sensitive to the hyper-parameters chosen. It takes work to find the
#     optimal parameters for a data set than to run the recommendation algorithm. Use the training and testing
#     datasets to evaluate various permutations of model parameters.
#   * Just because ML algorithms are complicated does not mean they are always better. Putting your faith
#     on a black box algorithm is dodgy. Complicated isn't always better. A solution based on simple rules
#     and concepts programmed and run over the data might be better than or equally good in comparison to the ML algorithm.
#   * Never blindly trust results when analyzing Big Data. Small problems in algorithms become big ones. Very often
#     quality of the input data is the real issue!


# RUN: spark-submit .\movie-recommendations-als-dataframe.py 1
