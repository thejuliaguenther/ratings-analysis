from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#Creates a new Spark SQL 
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a tuple.
ratings_lines = sc.textFile("./ml-20m/ratings.csv")
parts = lines.map(lambda l: l.split(","))
# people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = sqlContext.createDataFrame(people, schema)