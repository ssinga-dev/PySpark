## Owner - ssinga ##
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

# using data files movieRatings.data and movieNames.data
def loadMovieNames():
    movieNames = {}
    # Mention the path where movieNames.data file exists
    with codecs.open("C:/SparkCourse/data/movieNames.data", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Broadcast objects to yhe executors, such that they are always there whenever needed
# Ship off what we want
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading movieRatings.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/data/movieRatings.data")

movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    # Get the object back
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column from movieNames.data using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab top 20 ratings
sortedMoviesWithNames.show(25, False)

# Stop the session
spark.stop()
