# Apart from single values like a line of text or a movie rating,
# RDDs can also hold structured information like Key - Value pairs (similar to a NoSQL database, in essence)
# For example: Number of Friends by Age. Instead of just a list of ages or a list of number of Friends, we can]
# store (age, number of friends), (age, number of friends) etc.

# Syntactically, this is nothing special in python, just map pairs of data into the RDD

# TRANSFORMATIONS ON KEY-VALUE RDDs
# reduceByKey(): combine values with the same key using some function eg, 'rdd.reducebyKey(lambda x, y: x + y )' adds them up
# REMEMBER: reduceByKey() takes an associative and commutative function that takes two Values of the same Key 
# as arguments. This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a “combiner” in MapReduce.
# Hence, (lambda x, y: x + y ) will simply add the two Values of the same Key
# groupByKey(): group values with the same key into a list
# sortByKey(): sort RDD by Key
# keys(), values(): Create an RDD of just the Keys or just the Values
# join(), rightOuterJoin(), leftOuterJoin(), cogroup(), subtractByKey() and other SQLish things on two Key-Value RDDs
# IMPORTANT: If you are not going to actually modify the Keys from your transformations on the Key-Value RDD, make
# sure to call mapValues() and flatMapValues() instead of just map() or flatMap() - This allows Spark to maintin
# the partitioning from the original RDD instead of having to shuffle the data around which is expensive computationally
# Also keep in mind that both mapValues() and flatMapValues() use only the Values (one Value per auxilliary function call) 
# of the Key-Value pair. This does  not mean the Keys are discarded, they are just not being modified or exposed.

# The input data: ID,name,age,number of friends

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# Function to parse every line and get a key-value pair of age and number of friends.
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3]) # Casting string to integer value for numeric calculations on this data
    return (age, numFriends) # return a key-value pair

# Read lines of the 'fakefriends.csv' file into single value RDD. Each line: ID,name,age,number of friends
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
# Convert each line into a (age, numFriends) pair and create the Key - Value pair RDD called 'rdd'
# The Value can be a complex data type like a list of many elements as well.
rdd = lines.map(parseLine)
# mapValues(): Pass each value in the key-value pair RDD through a map function without changing the keys; 
# this also retains the original RDD’s partitioning.
# Here, each Value is converter to a Key-Value pair with Value as 1. (33, 385) => (33, (385, 1))
# Then we find the total of the value pair using reduceByKey(): We sum both the numFriends as well as the number
# of times a particular Key (age) occured. This will help us calculate the average as total(numFriends)/total(count)
# First action method called (reduceByKey()) - Spark evaluation triggered here
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) 
# Divide the Total number of Friends by the total count (total of the second value in the Value tuple) for each Value
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# Average number of Friends by Age - created as a python dictionary with age as Key and average number of friends as Value
results = averagesByAge.collect() # Action method to get a python object (dictionary) with the age and average number of friends
# results.sort() # Sort the dictionary entries by the Key (Age) - INPLACE SORT
# sort the dictionary entries by the Average Number of Friends in Descending Order - INPLACE SORT
results.sort(key= lambda x: x[1], reverse=True) 
for result in results:
    print(result)
