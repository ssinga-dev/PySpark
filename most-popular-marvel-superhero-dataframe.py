## Owner - ssinga ##
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularMarvelSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/data/Marvel-Names")

lines = spark.read.text("file:///SparkCourse/data/Marvel-Graph")

# Count how many space separated numbers are in the line and subtract 1 to get total no. of connections
# Split hero ID from the beginning of the line
# Group by hero IDs to add up connections split into multiple lines
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Sort total conditions by descending order
mostPopular = connections.sort(func.col("connections").desc()).first()

# Filter name lookup dataset by the most popular hero ID to look up the name
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")
