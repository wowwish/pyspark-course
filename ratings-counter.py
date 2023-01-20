# Spark is a fast and general engine for large-scale data processing
# The driver script is code written in Python, Java, or Scala that can be scaled across multiple executor
# nodes / worker nodes and managed by the cluster manager (head node). Spark can be used with its default 
# cluster manager or using YARN (Hadoop cluster manager). 
# Multiple executor nodes can be in the same machine (ideally, one executor just uses up one CPU core)
# SPARK can farm out and distribute the work from the driver code into multiple executors and simultaneously
# provide fault-tolerance, should some executors fail during the farming process, preventing the need to 
# execute the job all over again.
# SPARK is better than Hadoop MapReduce because of faster run times and optimization of workflows using 
# Directed Acyclic Graph (DAG) engine.
# Spark is built around one main concept: the Resilient Distributed Dataset (RDD)
# On top of Spark core, there are several other libraries that run on top of it including: Spark Streaming, Spark ML,
# Spark SQL, SparkGraph etc.


# RDD:
# It is an abstraction for a giant set of data

# sc:
# The 'sc' object is the SparkContext object created by the driver program and this object is responsible
# for making the RDDs resilient and distributed!
# eg: nums = parallelize([1, 2, 3])
# eg: sc.textFile("file:///c:/SparkCourse/ml-100k/u.data")
# Here 'file://' can be 's3n://' or 'hdfs://' for AWS S3 or Hadoop file systems
# You can also create RDDs from Hive: 
# hiveCtx = HiveContext(sc)
# rows = hiveCtx.sql("SELECT name, age FROM users") 
# You can also create RDDs from JDBC, Cassandra, HBase, ElasticSearch, JSON, CSV, sequence files, objects files, compressed formats

# TRANSFORMATION - ONE OR MORE RDDs INTO ANOTHER RDD
# Basic Transformation Operations on RDD:
# map - apply a function on the data values of an RDD and transform it into another RDD
# flatmap - similar to 'map', but can produce multiple values or no value for every data value in the input RDD. 
#           Can produce more or less data values compared to the original RDD.
# filter - trim out information from the RDD.
# distinct - get the unique data values in an RDD and throw out all the duplicates
# sample - randomly sample the data values of the RDD, get a smaller data set to work with or experiment on
# union, intersection, subtract, cartesian - set operations and joins on two RDDs

# ACTION METHODS - ONE RDD INTO A PYTHON OBJECT
# Actions that can be performed on an RDD when it has the dataset that you want
# collect - dump out all the values in the RDD
# count - count all the values in the RDD
# countByValue - get a breakdown of unique values and how many times they occur in the RDD
# take - sample the RDD final results
# top - sample the RDD final results
# reduce - use a function that combines all the different values for a given key value and perform a sort of 
#          summation or aggregation of the RDD.
# and more ...

# LAZY EVALUATION: Nothing actually happens in your driver program until an action is called! The driver script
# will not do anything untill one of these action methods are called. Once an action method is called, the processing
# occurs as Spark constructs a directed acyclic graph and farms things out.

# RUN: spark-submit ratings_counter.py

# NEW FEATURES IN Spark3:
# Spark3 has deprecated support for Python2 and Spark3 is much more highly performant than Spark2
# Spark ML library is the most recent and most relevant machine-learning library to use (MLLib is deprecated)
# Spark3 can take advantage of GPU and deeper Kubernetes Support
# Spark3 supports binary files - the entire content a binary file can be loaded into the row of a dataframe
# Spark3 introduces SparkGraph which allows to work on Graph data structures using the Cypher programming language
# SPAEK3 now has ACID support for data lakes through DeltaLake

# TO DO: Learn Scala to use Spark in its 'native' language and get to use Scala-first libraries

# Boiler Plate code for every driver script - SparkContext to create RDDs and SparkConf to configure the SparkContext
from pyspark import SparkConf, SparkContext
import collections # imported for standard python stuff for sorting and cleaning results

# Master node is set as the local machine - run on the local machine only - run on a single thread as a single
# process without any distribution across all the cores available in this machine.
# Set the App name to "RatingsHistogram" for viewing the process in the Spark Web UI - A Job name to identify the process
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") # Another piece of BoilerPlate - creating the SparkConfig
sc = SparkContext(conf = conf) # Another piece of BoilerPlate - creating the SparkContext

# Common way to create a RDD from hard-coded data
rdd  = sc.parallelize([1, 2, 3, 4])
rdd.map(lambda x: x*x) # square every element of the RDD

# Load up the data file - read the text file line by line and store each line as one data value in the 'lines' RDD
# NOTE: The columns in 'u.data' correspond to: user-id,movie-id,rating,timestamp of rating
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# split each data value of the 'lines' RDD by spaces and extract the third element for each line in the RDD (rating)
# and save it into a new RDD called 'ratings' (otherwise the transformed data will be lost - lines RDD is untouched)
ratings = lines.map(lambda x: x.split()[2])
# Count the occurrences / frequency of each unique value from the ratings column that was extracted before into the new 'ratings' RDD
# and save it into a python object (Since an action method is used here, it returns a python object instead of an RDD)
result = ratings.countByValue()

# Sort the results dictionary 'result' and print its values after sorting it
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
