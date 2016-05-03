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
    StructField("time_stamp", StringType(), True)])

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
    StructField("time_stamp", StringType(), True)])

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


mappedTitles = movieRowsRdd.map(lambda x: (str(x.title)[:-6],str(x.movieId)))
titleDict = mappedTitles.collectAsMap()



movieFile = open('movies.json', 'w')
movieJSON =  json.dump(movieDict, movieFile)


# # Save the tags to a JSON file 
tagFile = open('tags.json', 'w')
tagJSON = json.dump(tagDict, tagFile)


# Get the total number of ratings for a given movie 
ratingRows = sqlContext.sql("SELECT movieId, time_stamp from ratings")
ratingRowsRdd = ratingRows.rdd
# mappedRatings = ratingRowsRdd.map(lambda x: (str(x.movieId),str(x.time_stamp)))
# ratingDict = mappedRatings.countByKey()
numRatings = sorted(ratingRowsRdd.countByKey().items())
mappedRatings = {}

for rated_movie in numRatings:
    mappedRatings[rated_movie[0]] = rated_movie[1]

ratingsFile = open('ratings.json', 'w')
ratingsJSON =  json.dump(mappedRatings, ratingsFile)

timestampsPerMovie = ratingRowsRdd.map(lambda x: (str(x.movieId), str(x.time_stamp)))

# combinedTimestampsPerMovie = timestampsPerMovie.reduceByKey(lambda a,b: a+b)
timestampsPerMovieDict = timestampsPerMovie.collectAsMap()

print timestampsPerMovieDict

# print timestampsPerMovieDict

timestampsPerMovieFile = open('timestamps_per_movie.json', 'w')
timestampsPerMovieJSON =  json.dump(timestampsPerMovieDict, timestampsPerMovieFile)

# # Get the timestamps for ratings of each movie
# timestampRows = sqlContext.sql("SELECT movieId, tag, time_stamp from tags")
# timestampRowsRdd = timestampRows.rdd

# # Collect the id, tag, and time stamp for each rating 
# timestampRatings = timestampRowsRdd.map(lambda x: (str(x.movieId), (str(x.time_stamp), [str(x.tag)])))
# timestampRatingsDict = timestampRatings.collectAsMap()

# timestampRatingsFile = open('timestamp_ratings.json', 'w')
# timestampRatingsJSON = json.dump(timestampRatingsDict, timestampRatingsFile)








# for line in ratingDict:
#     print line 

# Visualize the timestamps for the movies over a time series 

    

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
 