# RUN

```
spark-submit <script_name>.py
```


# SPARK GraphX
The `GraphX` library of SPARK can only be used through the Scala language and is not implemented as part of pySpark. It provides functionality for calculating "Conectedness", degree distribution, average path length, triangle counts - just high level measures of a graph. It can also join graphs together and transform graphs quickly. 
Under the Hood, it introduces a couple of new data types called `VertexRDD` and `EdgeRDD`. For network analysis with SPARK, learning Scala is recommended!