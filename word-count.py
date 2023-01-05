# Biolerplate Code
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Reading the input text file - each line represents a whole paragraph of the book
input = sc.textFile("file:///sparkcourse/book.txt")
# map() transforms each element of an RDD into one new element. There is always a one-to-one relationship between the
# input RDD and the transformed RDD. flatmap() can create many new elements in the transformed RDD for each element in
# the original RDD.
words = input.flatMap(lambda x: x.split()) # Here, we are parsing the file as a whole. We are reading the entire text content
# as a single element RDD. Because of this, the same word is counter under differently in different places in the file due to associated
# punctuations, upper/lower case and whitespaces.
wordCounts = words.countByValue() # Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') # Take care of utf-8 encoded words for proper display and ignore any
    # conversion errors when transforming the utf-8 encoded word to ascii
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
