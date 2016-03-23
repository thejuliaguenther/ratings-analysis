from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy as np
from math import sqrt
import json 

conf = SparkConf().setMaster("local").setAppName("MovieRatingsApp")
sc = SparkContext(conf = conf)

#Creates a new Spark SQL 
sqlContext = SQLContext(sc)

ratingsSchema = StructType([ \
    StructField("userId", LongType(), True), \
    StructField("movieId", LongType(), True), \
    StructField("rating",  FloatType(), True), \
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


tagsSchema = StructType([ \
    StructField("userId", StringType(), True), \
    StructField("movieId", StringType(), True), \
    StructField("tag", StringType(), True), \
    StructField("timestamp", StringType(), True)])

tagsDF = sqlContext.read.format('com.databricks.spark.csv').options(quote="\"").load('./ml-20m/tags.csv', schema=tagsSchema)
tagsDF.registerTempTable("tags")


tagLines = sqlContext.sql("SELECT userId, movieId, tag FROM tags")
tagLinesRdd = tagLines.rdd


mappedTags = tagLinesRdd.map(lambda x: (str(x.movieId),[str(x.tag)]))

combinedTags = mappedTags.reduceByKey(lambda a,b: a+b)
tagDict = combinedTags.collectAsMap()

tagFile = open('tags.json', 'w')
tagJSON = json.dumps(tagDict, tagFile)

ratingLines = sqlContext.sql("SELECT userId, movieId, rating FROM ratings")
ratingLinesRdd = ratingLines.rdd


# 

# mappedRatings = ratingLinesRdd.map(lambda x: (str(x.userId), [(str(x.movieId),float(x.rating))]))

#Map each user to all of the movies rated by the user and the movie id of each movie

mappedRatings = ratingLinesRdd.map(lambda x: (long(x.userId), [(long(x.movieId),float(x.rating))]))
combinedRatings = mappedRatings.reduceByKey(lambda a,b: a+b)
ratingsDict = combinedRatings.collectAsMap()
# print combinedRatings.take(20)

#Create a numpy array of all of the user ids, movie ids, and ratings 
#
#Each rating is a separate line 

ratingsData = ratingLinesRdd.map(lambda x: np.fromstring(str(x), dtype=np.float64, sep=" "))


#Create an RDD for movie ratings by user
#Compare similar movies 
#maybe do kmeans 


clusters = KMeans.train(ratingsData, 4, maxIterations=10,
        runs=10, initializationMode="random")
print type(clusters)

# print clusters

# Evaluate clustering by computing Within Set Sum of Squared Errors
# def error(point):
#     center = clusters.centers[clusters.predict(point)]
#     return sqrt(sum([x**2 for x in (point - center)]))

# WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
# print("Within Set Sum of Squared Error = " + str(WSSSE))