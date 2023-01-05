# Boilerplate stuff - importing spark config and setting up pyspark
from pyspark import SparkConf, SparkContext
# run (on 2 threads) in local machine, create an Application Name for the job (for tracking the job in SPARK GUI)
conf = SparkConf().setMaster('local[2]').setAppName("totalSpendByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    """
    Function to parse each line in the input file and return a customer - amount key/value pair
    Each line in the input file contains customerID,itemID,amountSpentOnItem
    """
    fields = line.split(',')
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer, amount)
    

# Each line in the input file contains customerID,itemID,amountSpentOnItem
# Read the input CSV file as a text file and create an a RDD
input = sc.textFile("file:///SparkCourse/customer-orders.csv")
# Apply the line parsing function to each element of the RDD (each line of the input file)
# Spark RDD reduceByKey() transformation is used to merge the values of each key using an associative 
# reduce function. Here, we total the values of each unique key and store the results in a new RDD.
customerSpending = input.map(parseLine).reduceByKey(lambda x,y: x + y)
# Swap the key and value in each element pair of the RDD and sort by the key (total amount spent) - create new transformed RDD
customerSpendingSorted = customerSpending.map(lambda x: (x[1], x[0])).sortByKey()
results = customerSpendingSorted.collect() # Get final RDD elements into a python iterable (list)

for result in results: # pretty printing the sorted pairs from the RDD
    totalAmount = str(round(result[0], 2))
    customerID = str(result[1])
    print(customerID + ":\t\t" + totalAmount)


