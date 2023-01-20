# Predicting Real Estate values with Decision Trees based Regression in Spark
# Unlike Linear Regression, Decision Tree based Regression can handle data of different scale better and hence
# we don't have to scale the features ourself.


from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler # A feature transformer that merges multiple columns into a vector column.

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

    
    # Load up data as dataframe directly with the default inferred Schema (since header row is available for the file)
    data = spark.read.option("header", "true").option("inferSchema", "true")\
        .csv("file:///SparkCourse/realestate.csv")

    # Give the VectorAssembler a list of input columns that you want to assemble as your features and also
    # the output columns
    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", \
                               "NumberConvenienceStores"]).setOutputCol("features")
    # use 'assembler.transform()' to give the DataFrame to be transformed to the assembler
    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our decision tree - the decision tree can do without any hyper-parameters.
    # We specify the features column and the label column names for the input DataFrame here.
    dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

    # Train the model using our training data
    model = dtr.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our decision tree model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions and the "known" correct labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session
    spark.stop()
