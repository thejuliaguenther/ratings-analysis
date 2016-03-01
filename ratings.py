from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MovieRatingsApp")
sc = SparkContext(conf = conf)

#Creates a new Spark SQL 
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a tuple.
ratingsLines = sc.textFile("./ml-20m/ratings.csv")
parts = ratingsLines.map(lambda l: l.split(","))
ratings = parts.map(lambda p: (p[0], p[1], p[2], p[3].strip()))

# The schema is encoded in a string.
ratingsSchemaString = "userId movieId rating timestamp"

fields = [StructField(field_name, StringType(), True) for field_name in ratingsSchemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaRatings = sqlContext.createDataFrame(ratings, schema)

# Register the DataFrame as a table.
schemaRatings.registerTempTable("ratings")

# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT userId FROM ratings")

# The results of SQL queries are RDDs and support all the normal RDD operations.
ids = results.map(lambda p: "UserID: " + p.userId)
for userId in ids.collect():
  print(userId)