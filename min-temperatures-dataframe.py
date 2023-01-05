# DEBUGGING SPARK SCRIPTS IS VERY DIFFICULT BECAUSE:
#   * NOTHING REALLY HAPPENS UNTIL AN ACTION FUNCTION IS USED (LAZY EVALUATION)
#   * ALL THE OPERATIONS ARE DISTRIBUTED ACROSS AN ENTIRE CLUSTER AND DONOT RUN LOCALLY

# Importing usual stuff for DataFrames
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
# import data types to use with user-provided schema for reading a structured data file
# StructType and StructField are imported to create the schema structure
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Boilerplate for working with DataFrames 
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# Here, we provide our own schema to SparkSession.read() - our CSV file lacks header row
# The schema for reading the file will be built with a structure defined as a StructType 
# The StructType onject holds a list of individual fields (StructField) that we want to assign to our incoming data
# Each field is initialed as a StructField object with a field name, data type and a boolean representing 
# whether null values are allowed
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# // Read the CSV file as dataframe with our schema applied to the columns
df = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
df.printSchema()

# Filter out all but TMIN entries - using an epression as a parameter to the filter() method
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate by stationID to find minimum temperature for every station - this column will now be called "min(temperature)"
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show() # DEBUGGING STEP TO CHECK PROGRESS (.show() IS NOT USUALLY PART OF PRODUCTION CODE TO SAVE RESOURCES)

# Convert temperature to fahrenheit and sort the dataset
# The withColumn() method is used to add additional information to our DataFrame and this DataFrame with
# additional information is saved as the new minTempsByStationF DataFrame here.
minTempsByStationF = minTempsByStation.withColumn("temperature", # create a new column/field called temperature that contains the result of the following
                    # We use func.col("min(temperature)") here to refer to the column of that name created by our previous operations
                    func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                    .select("stationID", "temperature").sort("temperature") # select the stationID and new 
                    # temperature column and sort the results by the new temperature column
                                                  
# Collect into a python data object, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
# Stop the SparkSession
spark.stop()

                                                  