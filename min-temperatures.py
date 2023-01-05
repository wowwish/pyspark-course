# Standard Boilerplate Code
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# Function to parse each line of input CSV file
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # Convert temperature from tenths of a degree Celcius to degree Celcius (multiply by 0.1) and then to Farenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# Create RDD with each line of the input CSV file as an element
lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
# filter() removes data from your RDD. It takes a function that returns boolean as argument.
# Here, we filter out lines that don't have 'TMIN' in the second item of the tuple returned by "parseLine()" and create a filtered RDD
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# Next, we use map() to transform the 3-tuples in the RDD into a key-value pair RDD from the pairs (tuples) containing the
# station ID as key and the temperature measurement as value.
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# Next, we use 'reduceByKey()' to find minimum temperature by 'stationID', which corresponds to the
# keys in the key-value pair RDD. We provide a lambda function that returns the minimum value of two temperature measurements.
# Spark RDD reduceByKey() transformation is used to merge the values of each key using an associative reduce 
# function.  It is a wider transformation as it shuffles data across multiple partitions and it operates on 
# pair RDD (key/value pair).
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# Now we have our final RDD that one entry for every 'stationID' that contains the minimum temperature value for 
# that station. We create a python data object from calling the 'collect()' method to dump the data of the RDD.
results = minTemps.collect();

for result in results:
    # Print the minimum temperature of each station using some fancy formatting
    print(result[0] + "\t{:.2f}F".format(result[1]))

# RUN: spark-submit min-temperatures.py
# ITE00100554     5.36F
# EZE00100082     7.70F