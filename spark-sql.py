# SPARK SQL contains the DataFrame API which people prefer over RDD. A DataFrame object extends the RDD to a 
# Dataframe which:
#   - contains Row objects
#   - allows to run SQL queries against them (you can distribute an SQL query command across an entire cluster to 
#     query massive datasets - such queries might be too large to run on a traditional vertically scaled relational database)
#   - can have a schema (leading to more efficient storage)
#   - dataframes can be easily read from and writen to file formats like JSON, Hive, parquet, csv ... 
#   - can also be used to communicate with tools outside of SPARK ecosystem such as JDBC/ODBC, Tableau etc.


# In SPARK 2+, a DataFrame is is really a DataSet of Row objects
# DataSets can wrap known, typed data too. But this is mostly transparent to you in Python since Python is untyped.
# In Scala however, you'll want to use DataSets whenever possible becuse Scala being a statically typed language
# can store typed DataSets more efficiently and DataSets can also be optimized at compile time in Scala.

# POWER OF A SQL DATABASE WITH AN ENTIRE CLUSTER UNDER ITS HOOD - SHELL ACCESS:
# SPARK SQL exposes a JDBC/ODBC server (if you built SPARK with Hive support)
# Start it with sbin/start-thriftserver.sh
# Listens on port 10000 by default
# Connect using bin/beeline -u jdbc:java:hive2//localhost:10000 and you will have a SQL shell for SPARK SQL
# You can create new tables, or query existing ones that were cached using hiveCtx.cacheTable("tableName")

# FANCY STUFF WITH USER DEFINED FUNCTIONS (UDFs):
# from pyspark.sql.types import IntegerType
# def square(x):
#   return x * x
# Register the UDF with a name, the actual function and its input data type
# spark.udf.register("square", square, IntegerType())
# df = spark.sql("SELECT square('someNumericField') FROM tableName")

# SPARK SQL lives in SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession to expose the SPARK SQL interface using SparkSession.builder.appName()
# getOrCreate() allows us to connect back into persistent SparkSessions already present, or create a new SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# To load a JSON data file into a DataFrame of Row objects (like a database)
# inputData = spark.read.json(dataFile)

# Expose the dataframe as a virtual database table - create a temporary view on the dataframe (a typical SQL View) with a view name: myStructuredStuff
# inputData.createOrReplaceTempView('myStructuredStuff')

# Issue SQL commands on the database view that was created - you will get back the query results as another DataFrame
# myResultDataFrame = spark.sql("SELECT foo FROM bar ORDER BY foobar")

# METHODS THAT CAN BE CALLED DIRECTLY ON DATAFRAMES TO EMULATE SQL COMMANDS (THESE METHODS ALSO RETURN DATAFRAMES):
# myResultDataFrame.show(): similar to SELECT * FROM table
# myResultDataFrame.select("someFieldName"): similar to SQL's SELECT field FROM table
# myResultDataFrame.filter(myResultDataFrame("someFieldName" > 200)): similar to SELECT field FROM table WHERE condition
# myResultDataFrame.groupBy(myResultDataFrame("someFieldName")).mean(): similar to SELECT AVG(field1) FROM table GROUPBY field2
# myResultDataFrame.rdd().map(mapperFunction): convert a DataFrame object back to RDD and apply mapperFunction to every row


# Function to parse each line in the CSV file and return a RDD of Row objects for each line
def mapper(line):
    fields = line.split(',')
    # We define the individual fields, their names, their value and their data types for each Row object 
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

# Create a RDD of lines from the input text file. We have access to RDD API even in SparkSession
lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper) # Create an RDD of 

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache() # Create DataFrame from an RDD and cache the DataFrame in memory
schemaPeople.createOrReplaceTempView("people") # Create a temporary table view of the DataFrame

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are DatFrames and support all DatFrame operations.
for teen in teenagers.collect(): # Collect the Row objects of the DataFrame into a python list
  print(teen)

# We can also use functions instead of SQL queries:
# Group by records in the DataFrame by the age field and count number of Row records for each unique age value, 
# then order by ascending order of the age field and show the results as a DataFrame table
schemaPeople.groupBy("age").count().orderBy("age").show() # .show() only shows the first 20 records by default

# Stop the current SparkContext - if the current SparkSession is not stopped, it will be persistent and a new
# SparkSession will not be created when you run this script again.
spark.stop()
