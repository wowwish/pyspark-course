# SPARK STREAMING USING DSTREAMS:
#   * Analyzes continual streams of data 
#       - common example: processing log data from a website or server
#   * Data is aggregated and analyzed at some given interval
#   * Can take data fed to some port, Amazon Kinesis, HDFS, Kafka, Flume and Others
#   * "Checkpointing" stores state to disk periodiacally for fault tolerance
#   * A "DStream" object breaks up the stream into distinct RDD's (microbatches of data chunks at a time dealt with continuously)
#     A "DStream" is not real-time streaming! There will always be a short time period delay between successive batches of data being processed.
#   * A simple word-count Example:
#     - This looks for text files dropped into the "books" directory and counts up how often each word
#       appears over time, updating every one second (StreamingContext(sc, 1)):
#
#           ssc = StreamingContext(sc, 1)
#           lines = ssc.textFileStream("books")
#           counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1).reduceByKey(lambda a, b: a + b))
#           counts.pprint()
#
#     - You need to kickoff the job explicitly:
#
#           ssc.start()
#           ssc.awaitTermination()
#
#   * Remember, your RDDs only contain one little chunk of incoming data.
#   * "Windowed Operations" can combine results from multiple batches over some sliding time window
#       - see window(), reduceByWindow(), reducyByKeyAndWindow()
#   * updateStateByKey() lets you maintain a state across many batches as time goes on. For example, running
#     counts of some event. See examples in SDK Documentation!


# NEW METHOD OF STREAMING DATA - STRUCTURED STREAMING:
#   * Incremental data analysis and updation of results - Suppose two data packets arrive at t=1 and t=2, first
#     the result will be generated at t=1 for data up to t=1, next, result will be generated at t=2 with data
#     up to t=1 appended with data between t=1 and t=2
#   * Models streaming as a DataFrame that just keeps expanding - data stream as an unbounded input table.
#   * Python support now available
#   * Streaming DataFrame code ends up looking a lot like non-streaming DataFrame code
#   * Interoperability with SPARK.ML


# Boilerplate stuff
from pyspark import SparkContext
from pyspark.streaming import StreamingContext # For using Streaming DataFrames rather than normal DataFrames
from pyspark.sql import Row, SparkSession
# For regular expressions usage
from pyspark.sql.functions import regexp_extract

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
logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count of every access by HTTP status code
statusCountsDF = logsDF.groupBy(logsDF.status).count()

# Kick off our StreamingQuery "query" (A handle to a query that is executing continuously in the background as 
# new data arrives), dumping results to the console using .writeStream()
# Here, we want to dump the complete output to the console with a StreamingQuery name of "counts"
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

# CREATE A "logs" SUB-DIRECTORY IN THE DIRECTORY WHERE YOU ARE RUNNING THIS DRIVER SCRIPT FOR IT TO MONITOR FOR
# INPUT LOG FILES
# IF java.lang.UnsatisfiedLinkError ARISES IN WINDOWS, MAKE SURE TO COPY 'hadoop.dll' from winutils to 'C:/Windows/System32/'
# RUN: spark-submit structured-streaming.py
# COPY 'access_log.txt' INTO THE 'logs' SUB-DIRECTORY TO SEE THE JOB PICKUP THE NEW TEXT FILE AND DISPLAY THE RESULTS.
# CTRL+C TO TERMINATE
