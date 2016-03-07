from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MovieRatingsApp")
sc = SparkContext(conf = conf)

#Creates a new Spark SQL 
sqlContext = SQLContext(sc)

ratingsSchema = StructType([ \
    StructField("userId", StringType(), True), \
    StructField("movieId", StringType(), True), \
    StructField("rating", StringType(), True), \
    StructField("timestamp", StringType(), True)])

# Load a text file and create a new DataFrame from the text file 
ratingsDF = sqlContext.read.format('com.databricks.spark.csv').options(quote="\"").load('./ml-20m/ratings.csv', schema=ratingsSchema)
ratingsDF.registerTempTable("ratings")

moviesSchema = StructType([ \
    StructField("movieId", StringType(), True), \
    StructField("title", StringType(), True), \
    StructField("genres", StringType(), True)])

moviesDF = sqlContext.read.format('com.databricks.spark.csv').options(quote="\"").load('./ml-20m/movies.csv', schema=moviesSchema)
moviesDF.registerTempTable("movies")


results = sqlContext.sql("SELECT * FROM movies")

types = results.map(lambda p:"Movie ID "+p.movieId+ " Genres: " + p.genres)
for genre in types.collect():
  print(genre)