# In this example, we use a more typical structured data file
# We have a header Row in the input CSV file

# Importing SparkSession for DataFrame usage
from pyspark.sql import SparkSession
# Setup the SparkSession - not using SparkContext here, using only SPARK SQL and no RDDs (which require SparkContext)
# Create a SparkSession to expose the SPARK SQL interface using SparkSession.builder. Use appName() to set the app name for SPARK Web UI
# getOrCreate() allows us to connect back into persistent SparkSessions already present, or create a new SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Reading the CSV file directly using SparkSession - specifying that the input file contains a header row.
# The inferSchema option tells SPARK to figure out the Schema for the DataFrame from the file contents.
# Then we user the .csv() method to parse the input CSV file.
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")

# Now we have a DataFrame with an inferred schema. We can print that inferred schema.
print("Here is our inferred schema:")
people.printSchema()

# The column names of the DataFrame and their types (Schema) is inferred during Runtime.
print("Let's display the name column:")
people.select("name").show() # .show() only shows the first 20 rows by default when no limit parameters are passed to it

# Boolean expression passed to the .filter() method of DataFrame actually works (here, "people.age" refers to the "age" field of
# the DataFrame that will be inferred at runtime!)
print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

# Get the age-wise frequency of people and show 20 records (default number of records shown by .show()) from that
print("Group by age")
people.groupBy("age").count().show()

# Display the name and age + 10 values for the first 20 rows (only display it, not saved anywhere)
print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

# Stop the SparkSession/SparkContext when everything is done
spark.stop()

