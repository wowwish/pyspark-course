# LINEAR REGRESSION:
#   * Fit a line to a DataSet of observations
#   * Use this line to predict unobserved values
#   * Uses the technique of "least squares" - minimizes the squared-error between each data point and the line
#   * y = mx + b
#   * You train the model and make predictions using tuples that consist of a label and a vector of features.
#     In this case, the "label" is the value you're trying to predict - usually your Y axis, and the "feature"
#     is your X axis or other axes. 
#   * So, you train the model with a bunch of known (X, Y) points and predict new Y values for the given X's
#     using the line that the model created. X can have more than two dimensions (multiple features)
#   * SPARK uses Stochastic Gradient Descent (SGD) algorithm to fit the line by minimizing the squared-errors (residuals).
#   * IMPORTANT: You need to "scale" your data because SGD doesn't handle feature scaling well. It assumes your data
#     to be similar to a normal distribution. So you need to scale your data features down to a uniform scale 
#     before training the model and then scale your results back up. The model also assumes that your y-intercept
#     is zero unless you use "fitintercept(true)" in the model properties.

# Can we use Linear Regression to predict how much people spend on a Website based on how fast their
# Web page load times were (Page Speed / Responsiveness of Website) ?

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up our data and convert it to the format SparkML expects (label/target_variable, (Vector of floating-point features)).
    inputLines = spark.sparkContext.textFile("regression.txt")
    data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    # Convert this RDD to a DataFrame
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    # Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    # Perhaps you're importing data from a real database. Or you are using structured streaming
    # to get your data.

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our linear regression model
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Train the model using our training data
    model = lir.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions and the "known" correct labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session
    spark.stop()

# KEEP IN MIND THAT BOTH THE PREDICTION AND ACTUAL VALUE HAVE BEEN SCALED DOWN HERE AND WILL HAVE TO BE SCALED
# UP TO REFLECT THEIR TRUE VALUE.
# RUN: spark-submit spark-linear-regression.py