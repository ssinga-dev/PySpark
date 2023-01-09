from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureMarvelSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/data/Marvel-Names")

lines = spark.read.text("file:///SparkCourse/data/Marvel-Graph")

# Count how many space separated numbers are in the line and subtract 1 to get total no. of connections
# Split hero ID from the beginning of the line
# Group by hero IDs to add up connections split into multiple lines
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
minConnectionCount = connections.agg(func.min("connections")).first()[0]

# Filter the connection to find rows with only one connection
minConnections = connections.filter(func.col("connections") == minConnectionCount)

# Join the results with the names dataframe
minConnectionsWithNames = minConnections.join(names, "id")

print("The following characters have only " + str(minConnectionCount) + " connection(s):")
# select the names column
minConnectionsWithNames.select("name").show()