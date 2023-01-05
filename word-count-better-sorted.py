import re # for using regular expressions
from pyspark import SparkConf, SparkContext

# Function to take care of counting of same words differentiated by punctuation, capitalization or whitespaces.
def normalizeWords(text):
    # Create a regular-expression pattern. We convert the text to lowercase and split the word based on non-word characters
    # full words in the text. '\W' in regular-expression syntax matches Matches any character which is not a word 
    # character. This is the opposite of '\w'. If the re.ASCII flag is used this becomes the equivalent of '[^a-zA-Z0-9_]'
    # Here, '\W+' is matching one or more consecutive non-unicode non-word characters due to the re.UNICODE flag.
    return re.compile(r'\W+', re.UNICODE).split(text.lower()) # Take care of capitalization issue by converting whole text to lowercase

# Set the SPARK configuration and name for the Job
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Read the input text file
input = sc.textFile("file:///SparkCourse/book.txt")
# Custom function used in flatmap() to transform input RDD
words = input.flatMap(normalizeWords) # get individual words using regular expression from the file content as a whole
# by splitting the entire file text by non-word characters

# Doing countByValue() the hard way - convert each word to a key/value pair with a value of 1, then count them
# up by reduceByKey(). Spark RDD reduceByKey() transformation is used to merge the values of each key using an 
# associative reduce function.
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey() # swap key and value, then sort 
# by the key (count) in ascending order (default behaviour)
results = wordCountsSorted.collect() # convert the RDD to python data object (list)

for result in results:
    # Remember that after swapping the key/value pairs now the count is at index 0 and the word is at index 1 in the pair
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore') # Take care of utf-8 encoded words for proper display and ignore any
    # conversion errors when transforming the utf-8 encoded word to ascii
    if (word):
        print(word.decode() + ":\t\t" + count) # Pretty printing
