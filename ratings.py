from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy as np
from math import sqrt
import json 

conf = SparkConf().setMaster("local").setAppName("MovieRatingsApp")
sc = SparkContext(conf = conf)

sqlContext = SQLContext(sc)

# These lines set up a schema representing the movie ratings data from the 
# ratings.csv file 
ratingsSchema = StructType([ \
    StructField("userId", LongType(), True), \
    StructField("movieId", LongType(), True), \
    StructField("rating",  FloatType(), True), \
    StructField("timestamp", StringType(), True)])

ratingsDF = sqlContext.read.format('com.databricks.spark.csv').options(quote="\"").load('./ml-20m/ratings.csv', schema=ratingsSchema)
ratingsDF.registerTempTable("ratings")

# These lines set up a schema representing the movie data from the 
# movies.csv file 
moviesSchema = StructType([ \
    StructField("movieId", StringType(), True), \
    StructField("title", StringType(), True), \
    StructField("genres", StringType(), True)])

moviesDF = sqlContext.read.format('com.databricks.spark.csv').options(quote="\"").load('./ml-20m/movies.csv', schema=moviesSchema)
moviesDF.registerTempTable("movies")

# These lines set up a schema representing the information on the 
# tags with which the users described movies from the tags.csv file 
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

# print tagDict

movieRows = sqlContext.sql("SELECT movieId, title from movies")
movieRowsRdd = movieRows.rdd

mappedMovies = movieRowsRdd.map(lambda x: (str(x.movieId),str(x.title)))
movieDict = mappedMovies.collectAsMap()

for line in movieDict:
    print line +":"+movieDict[line]



# # Save the tags to a JSON file 
# tagFile = open('tags.json', 'w')
# tagJSON = json.dump(tagDict, tagFile)

# def get_movie_by_id():
#     for tag in tagDict:

    

# ratingLines = sqlContext.sql("SELECT userId, movieId, rating FROM ratings")
# ratingLinesRdd = ratingLines.rdd

# Map each user to all of the movies rated by the user and the movie id of each movie
# mappedRatings = ratingLinesRdd.map(lambda x: (long(x.userId), [(long(x.movieId),float(x.rating))]))
# combinedRatings = mappedRatings.reduceByKey(lambda a,b: a+b)
# ratingsDict = combinedRatings.collectAsMap()

# # Create a numpy array of all of the user ids, movie ids, and ratings 
# # Each rating is a separate line 
# ratingsData = ratingLinesRdd.map(lambda x: np.fromstring(str(x), dtype=np.float64, sep=" "))

# # Perform the KMeans clustering algorithm on the movie ratings data
# clusters = KMeans.train(ratingsData, 4, maxIterations=10,
#         runs=10, initializationMode="random")

# print "CLUSTERS!!!!"
# print type(clusters)

# Save the clusters to a model and load it
# clusters.save(sc, "RatingsModelPath")
# sameModel = KMeansModel.load(sc, "RatingsModelPath")
 