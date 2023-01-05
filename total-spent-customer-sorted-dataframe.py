from pyspark.sql import SparkSession
# import build-in DataFrame methods
from pyspark.sql import functions as func
# Import schema builders and data types
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# Boilerplate to initialize a SparkSession to work with SPARK DataFrames
# Here, we use .master() method to specify the Job to run on all available CPU cores of this system (implied by "local[*]")
spark = SparkSession.builder.appName("TotalSpendByCustomer").master("local[*]").getOrCreate()

# Defining our schema for the input CSV file
customerOrderSchema = StructType([ \
    StructField("customerID", StringType(), True), \
    StructField("itemID", StringType(), True), \
    StructField("amount", FloatType(), True)])

# Read input CSV file using our schema
df = spark.read.schema(customerOrderSchema).csv("file:///SparkCourse/customer-orders.csv")
# Keep only the relevant fields of DataFrame to save up on resources
customerPurchase = df.select("customerID", "amount")
# Aggregate by customerID and find the total amount spent by each customer, sort result by amount spent
customerByTotalSpendSorted = customerPurchase.groupBy("customerID").agg(func.round(func.sum("amount"), 2)\
                                .alias("amount_spent")).sort("amount_spent", ascending=False)
# Display all records of the final DataFrame
customerByTotalSpendSorted.show(customerByTotalSpendSorted.count())

# Stop the SparkSession
spark.stop()