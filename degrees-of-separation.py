# HERE, WE USE BREADTH-FIRST-SEARCH (BFS) TO DETERMINE THE DEGREES OF SEPERATION BETWEEN HEROES IN THE CONNECTION
# GRAPH. 
# STEPS:
#   * Represent each line of "Marvel-graph.txt" as a node with connections, a color, and a distance. 
#       Example: The line '5983 1165 3836 4361 1282' becomes '(5983, (1165, 3836, 4361, 1282), 9999, WHITE)'
#     Our initial condition is that a node is infinitely distant (9999) and white in color
#   * Go through the entire graph multiple times, using a breadth-first-search appproach, looking for gray nodes to expand
#   * Color the nodes that have been processed/expanded/visited with black and color the new nodes to be visited grey.
#   * Keep updating the distances to keep track of the degrees of seperation from the starting point as you 
#     traverse through the graph until you find the node of interest (Destination)

# A BREADTH-FIRST-SEARCH ITERATION AS A MAP AND REDUCE JOB
# THE MAPPER:
#   * Creates new nodes for each connection of gray nodes, with a distance incremented by 1, color grey and no connections
#   * Colors the grey node we just processed/visited black
#   * Copies the node itself into the results
# THE REDUCER:
#   * Combines together all nodes for the same hero ID (flatMap operation). This takes care of the multiple-lines with same
#     hero ID at the start problem of our input file.
#   * Preserves the shortest distance and the darkest color found for the hero ID (key).
#   * Preserves the list of connections (that we pass along when we copy the original node into the results) 
#     from the original node and marries that back into the other nodes of the current iteration.

# We go through iterations of the mapper and reducer functions over and over ultil we get to the hero character
# that we are interested in  

# An 'accumulator' allows many executors to increment a shared variable (like a shared counter). For Example:
#       hitCounter = sc.accumulator(0)
# sets up a shared accumulator with an initial value of 0. For each iteration, If the hero character we are interested
# in is hit, we increment the 'hitCounter' accumulator. After each iteration, we check if this hitCounter is greater
# than one - if so, we're done.

# Boilerplate stuff:
from pyspark import SparkConf, SparkContext
# Setting up the SparkConf for using RDDs
conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal. 
hitCounter = sc.accumulator(0)
# The sc.accumulator is a shared variable that can be incremented by workers/executors in
# the cluster using an ".add()" operation.(similar to "+=" operator, but only the driver program is allowed to access
# the value of this accumulator variable using ".value". Updates from the workers on this accumulator get propagated 
# automatically to the driver program.)
# While SparkContext supports accumulators for primitive data types like int and float, users can also define 
# accumulators for custom types by providing a custom AccumulatorParam object

# PARSER FUNCTION:
# Function to parse each line and create the structure outlined in the steps. 
# We create a key-value pair like: (first hero ID, (other connected IDs from the same comic book), distance, color)
# for each line in the input file.
def convertToBFS(line):
    # split the line by whitespaces
    fields = line.split()
    # get the ID of the hero of interest (the first ID in the line) 
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        # collect the connection IDs to the hero ID of interest
        connections.append(int(connection))
    # Set the color of current line to WHITE (visited)
    color = 'WHITE'
    # Set the distance of current line to a very high/improbable number
    distance = 9999

    # If our starting hero is equal to the hero from whom we are going to calculate the distancce, we set the
    # color to grey and distance to 0 - to indicate that this node needs to be processed and expanded upon in 
    # the first iteration of the breadth-first-search.
    if (heroID == startCharacterID): # REMEMBER: OUT INPUT FILE CONTAINS MULTIPLE LINES FOR THE SAME HERO ID
        color = 'GRAY'
        distance = 0
    # Each node in our data is represented like this:
    return (heroID, (connections, distance, color))

# Function to read input file and use the parser function that we defined to create the specific data structure
# out of the input file as a RDD.
def createStartingRdd():
    inputFile = sc.textFile("file:///sparkcourse/marvel-graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    # Map each element of the RDD  called 'node' to the hero ID, its connections, distance and color
    # Remember that each element is a key-value pair in the 'node' RDD.
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            # For the starting GRAY color node, distance will be 0
            newDistance = distance + 1
            newColor = 'GRAY' # Initialize the new nodes to be expanded with incremented distance and GRAY color
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry) # put all the new nodes to be expanded in 'results'

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) ) # save the current input node as well in 'results'
    return results

def bfsReduce(data1, data2):
    # REMEMBER: OUR INPUT FILE CONTAINS MULTIPLE LINES FOR THE SAME HERO ID
    # Since we use this function with .reduceByKey() method, the two arguments data1 and data2 will hold
    # only the values of two key-value pairs with the same key in the 'mapped' RDD. Hence, 'data1' and 'data2'
    # here will contain (connections, distance, color)
    edges1 = data1[0] # connections
    edges2 = data2[0] # connections
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    # Initializing the values for the combined node
    distance = 9999
    color = color1
    edges = []

    # combine the connections
    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance from the two nodes having the same hero ID (key)
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color of the two nodes having the same hero ID (key)
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


# Main program here:
# Create the RDD for the first iteration of BFS
iterationRdd = createStartingRdd()

# We use an arbitrary upper bound for the number of iterations (10)
for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated (counts the number of elements in the RDD),
    # and that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break
    
    # If we donot encounter the target hero ID, we combine the iteration's results and prepare for the next iteration
    # We use the reducer to combine two nodes of the same character ID using reduceByKey().
    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
