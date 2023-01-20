# OPTIMIZING FOR RUNNING IN A CLUSTER: PARTITIONING
# SCALING UP YOUR DRIVER CODE FOR VERY LARGE DATASETS

# Spark/PySpark partitioning is a way to split the data into multiple partitions so that you can execute 
# transformations on multiple partitions in parallel which allows completing the job faster.
# You can also write partitioned data into a file system (multiple sub-directories) for faster reads by downstream systems.
# Spark has several partitioning methods to achieve parallelism, based on your need, you should choose which one 
# to use.

# PySpark's partitionBy() is a function of pyspark.sql.DataFrameWriter class which is used to partition the 
# large dataset (DataFrame) into smaller files based on one or multiple columns while writing to disk. 
# Partitioning the data on the file system is a way to improve the performance of the query when dealing with a 
# large dataset in the Data lake. A Data Lake is a centralized repository of structured, semi-structured, 
# unstructured, and binary data that allows you to store a large amount of data as-is in its original raw format. 

# PySpark supports partition in two ways; partition in memory (DataFrame) and partition on the disk (File system).
# Partition in memory: You can partition or repartition the DataFrame by calling repartition() or coalesce() transformations. 
# Partition on disk: While writing the PySpark DataFrame back to disk, you can choose how to partition the data 
# based on columns using partitionBy() of pyspark.sql.DataFrameWriter. When you write PySpark DataFrame to disk 
# by calling partitionBy(), PySpark splits the records based on the partition column and stores each partition 
# data into a sub-directory. 
# Note: While writing the data as partitions, PySpark eliminates the partition column on the data file and adds 
# partition column & value to the folder name, hence it saves some space on storage.

# For each partition column, if you wanted to further divide into several partitions, use repartition() and 
# partitionBy() together as explained in the below example. repartition() creates specified number of partitions 
# in memory. The partitionBy()  will write files to disk for each memory partition and partition column.

# ALWAYS LOOK FOR PLACES IN YOUR CODE WHERE PARTITIONING WILL HELP WHEN RUNNING YOUR JOBS IN A CLUSTER!!!
# CHOOSING THE RIGHT PARTITION SIZE:
# The right partition size will make efficient use of your cluster. Too few partitions won't take full advantage
# of your cluster and cannot effectively spread out your data whereas too many partitions will shuffle your data
# into many smaller chunks that will lead to increased overhead (The overhead for running an individual executor 
# job multiplied by the number of too small data chunks). Ideally, you would want to have atleast as many partitions
# as the number of cores in your cluster (or as many partitions as the number of executors that can 
# fit in your cluster's available memory). partitionBy(100) is usually a reasonable place to start for large operations.


# WHEN EXECUTING SPARK JOBS IN A CLUSTER, THE DRIVER SCRIPT IS ALWAYS RUN IN THE MASTER NODE.

import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {} # The movieID - to - movieName lookup table
    # The 'movies.dat' file has to be copied along with this script 'MovieSimilarities1M.py' to the Master Node
    # for this driver script to properly read the input file. If the input 'movies.dat' file exists in another location
    # on the master node, specify the correct path for reading the file in the master node through this script.
    with open("ml-1m-movies.dat",  encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split("::") # The delimiter of fields in each line of this input file is '::'
            movieNames[int(fields[0])] = fields[1] # lookup table of movieID to movieName
    return movieNames

def makePairs( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


conf = SparkConf()
# Notice that the SparkContext is created without initializing it with the .setMaster() method or .setAppName()
# method, because these options will be provided through the command line when executing this driver script.
# In Amazor EMR, the pre-configured environment of the EMR will take care of these settings for you so that your driver
# script runs on the entire cluster on top of Hadoop Yarn, instead of just on the master node.
# Also when using SparkConf this way, you can tune the options through command-line. Such options include:
#   * Executor memory budget - by default, every executor is allocated a memory of 512 Mb. This may be insufficient
#     for processing given the size of your data, in which case, you can increase the memory allocation to each
#     executor using the command-line option --executor-memory 
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

# Loading the movie ratings data for MovieLens 1Million movies dataset from a Public Amazon S3 storage bucket
data = sc.textFile("s3n://sundog-spark/ml-1m/ratings.dat")

# Map ratings to key / value pairs: user ID => movie ID, rating - again, the field delimiter in each line is '::'
ratings = data.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# This self-join operation is expensive and cannot be done in a single-batch as we did in our
# other script: 'popular-movies-dataframe.py'. Spark won't distribute this compute expensive job on it's own.
# Here, we use the .partitionBy() function on the 'ratings' RDD to benifit this self-join operation. Other
# operations that benifit from partitioning include: .join() .cogroup() .groupWith() .join() .leftOuterJoin()
# .rightOuterJoin() .groupByKey() .reduceByKey() .combineByKey() and lookup(). Remember that these operations
# will preserve the partitioning in their results too!!
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()

# Save the results if desired
moviePairSimilarities.sortByKey()
moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    # Must increase this coOccurrenceTreshold to get better set of results since our dataset now has
    # 1 million records.
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))


# RUNNING ON A CLUSTER:
#   * Get your scripts and data someplace where EMR can access them easily. AWS S3 is a good choice - just use
#     's3n://<URL>' when specifying file paths, and make sure your file permissions make them accessible. 
#   * Then, spin up an EMR cluster for SPARK using the AWS console (BILLING STARTS NOW!!). 
#   * Get the external DNS name for the master node, and log into it using the "Hadoop" user account and your
#     private key file.
#   * Copy your driver program and any files it needs using aws-cli ('aws s3 cp s3://bucket-name/filename ./')
#   * Run spark-submit and watch the output!
#   * REMEMBER TO TERMINATE YOUR CLUSTER WHEN YOU ARE DONE!!

# RUN (from master-node of cluster): spark-submit --executor-memory 1g MovieSimilarities1M.py <movieID(260 for Star Wars)>
# Whether we need to change '--executor-memory' will depend on the version of SPARK and the hardware we choose.
# We might get lucky and not need to do this!
# You can also pass the '--master yarn' option to run on a Hadoop YARN cluster. AWS EMR sets this up by default.


# TROUBLESHOOTING SPARK CLUSTER JOBS:
# SPARK offers a Web console UI that runs on PORT 4040 IN THE STANDALONE MODE. This UI allows you to check:
#   * The log (Thread Dumps) of individual executors. 
#   * The directed acyclic graph (DAG) visualization of the SPARK job
#   * The Event Timeline of the submitted job, with metrics on each stage of the job timeline
#   * An 'Environment' Tab in the Web UI hold details on SPARK properties, system environment, paths, dependencies
#     and software versions installed.
# But in the Amazon EMR, it is tough to connect to this web UI from outside. However, if you have your 
# own cluster running on your own network, you can take a look at this console when throwing more memory at the 
# executors does not solve your errors and something deeper is going on.
# YOU CAN ACCESS THE SPARK WEB-UI AT 'localhost:4040' IN YOUR LOCAL SYSTEM AS WELL AFTER STARTING A SPARK JOB
# THAT RUNS FOR A COUPLE OF MINUTES AND CHECK OUT THE WEB UI.
# If you are running your jobs on YARN, the logs end up getting distributed around and you will have to collect
# them using 'yarn logs -appplicationID <app ID>'. 

# While your driver script is runs, it will log errors like executors (worker nodes) failing to issue heartbeats.
#   * This generally means, you are asking too much of each executor (worker node)
#   * You may need more of them - ie, more machines in your cluster
#   * Each executro may need more memory
#   * Or, use .partitionBy() to demand less work from individual executors by using smaller partitions

# MANAGING DEPENDENCIES:
#   * Different Java/Scala/python environment in the cluster compared to your driver script running in Desktop
#     (environment mismatch between head node (master/driver) and worker nodes (executors)).
#   * Use broadcast variables to share data across executors outside of RDDs. Use dstributed file systems to share
#     data between the files in the cluster and avoid the use of absolute paths for files.
#   * Need some python package that is not pre-loaded on EMR ? Set up a step in EMR to run 'pip install' of
#     the python packages you need on each executor / worker machine, or, use '--py_files <Comma-separated list of .zip, .egg, or .py files to place
#     on the PYTHONPATH for Python apps>' to add individual libraries that are on the master node to the executors. 