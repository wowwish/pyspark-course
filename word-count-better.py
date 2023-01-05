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
input = sc.textFile("file:///sparkcourse/book.txt")
# Custom function used in flatmap() to transform input RDD
words = input.flatMap(normalizeWords) # Here, we are parsing the file as a whole. We are reading the entire text content
# as a single element RDD.
wordCounts = words.countByValue() # Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') # Take care of utf-8 encoded words for proper display and ignore any
    # conversion errors when transforming the utf-8 encoded word to ascii
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
