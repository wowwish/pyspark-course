# WINDOWED COMPUTATIONS WITH SPARK STREAMING:
#   * A "windowed" operation looks back over some period of time. Every time the window slides over a source 
#     DStream, the source RDDs that fall within the window are combined and operated upon to produce the RDDs of 
#     the windowed DStream
#       - example: consider only the events that happened in the past 10 minutes
#       - the "slide interval" defines how often we evaluate a window
#       - the data for every possible window is re-built and at each slide interval
#       - for example, suppose you have a window size of 10 mins and a slide interval of 5mins and start the job
#         at 12:00, you will have the window 12:00-12:10 evaluated at 12:05 and then at 12:10 you will have two
#         windows 12:00-12:10 and 12:05-12:15 evaluated. At 12:15, we will evaluate three windows:
#         12:00-12:10, 12:05-12:15 and 12:10-12:20. These windows are stored in Stream DataFrames.
#   * Any "window" operation needs to specify two parameters.
#       - window length : The duration of the window.
#       - sliding interval : The interval at which the window operation is performed


# WINDOWED GROUPBY ERATION EXAMPLE USING func.window() :
# dataset.groupBy(func.window(func.col("timestampColumnName"), windowDuration = "10 minutes", 
# slideDuration = "5 minutes"),
# func.col("columnWeAreGroupingBy"))


# Boilerplate stuff
from pyspark import SparkContext
from pyspark.streaming import StreamingContext # For using Streaming DataFrames rather than normal DataFrames
from pyspark.sql import Row, SparkSession
# For additional built-in functions and regular expressions support
import pyspark.sql.functions as func


# Create a SparkSession (the .config() bit is only for Windows OS which sometimes causes issues with SPARK!)
# We use .getOrCreate() to resume from a previous checkpoint if any was created the last time we ran this script
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("logs") # Creates a Streaming DataFrame of the input data
# Searches for new/updated text files in the "logs" sub-directory

# Parse out individual log file lines and break it into its components - the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

# Use the above defined regular expression patterns to extract the particular content from each line and store in the DataFrame
# We add aliases as field/column names of the DataFrame
logsDF = accessLines.select(func.regexp_extract('value', hostExp, 1).alias('host'),
                         func.regexp_extract('value', timeExp, 1).alias('timestamp'),
                         func.regexp_extract('value', generalExp, 1).alias('method'),
                         func.regexp_extract('value', generalExp, 2).alias('endpoint'),
                         func.regexp_extract('value', generalExp, 3).alias('protocol'),
                         func.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         func.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# We use func.current_timestamp() to write the current timestamp to the new column "eventTime" in the DataFrame
# since the "timestamp" column of the input DataFrame is old its time format is not suitable.
logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())

# Keep a running track of the most viewed endpoint URL across a window time of 30 seconds and 10 second slide
endpointCountsDF =  logsDF2.groupBy(func.window(func.col("eventTime"), "30 seconds", "10 seconds"), \
                    func.col("endpoint")).count()
# Then, we order the window Stream DataFrames by the count of visits for each endpoint URL in descending order.
sortedEndpointCounts = endpointCountsDF.orderBy(func.col("count").desc())

# Kick off our StreamingQuery "query" (A handle to a query that is executing continuously in the background as 
# new data arrives), dumping results to the console using .writeStream()
# Here, we want to dump the complete output to the console with a StreamingQuery name of "counts"
query = ( sortedEndpointCounts.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()


# CREATE A "logs" SUB-DIRECTORY IN THE DIRECTORY WHERE YOU ARE RUNNING THIS DRIVER SCRIPT FOR IT TO MONITOR FOR
# INPUT LOG FILES
# IF java.lang.UnsatisfiedLinkError ARISES IN WINDOWS, MAKE SURE TO COPY 'hadoop.dll' from winutils to 'C:/Windows/System32/'
# RUN: spark-submit top-urls.py
# COPY 'access_log.txt' INTO THE 'logs' SUB-DIRECTORY TO SEE THE JOB PICKUP THE NEW TEXT FILE AND DISPLAY THE RESULTS.
# CREATE COPIES OF 'access_log.txt' AND COPY THEM TO THE 'logs' SUB-DIRECTORY AT 10 SECOND INTERVALS TO SEE THE OVERLAPPING WINDOWS!
# CTRL+C TO TERMINATE