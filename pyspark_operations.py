# Create a SparkSession with common .options() configurations
from pyspark.sql import SparkSession
spark = SparkSession.builder \
 .master("local[1]") \
 .appName("DataIngestion_With_CSV") \
 .config("spark.executor.memory", '3g') \
 .config("spark.executor.cores", '1') \
 .config("spark.cores.max", '1') \
 .getOrCreate()

# Read the spotify-2023 data
df = spark.read.option('header',True).csv('spotify-2023.csv')

# Show the data
df.show()

# Renamed column
df = df.withColumnRenamed("artist(s)_name", "artist_name")

# Select subset of columns
df = df.select("track_name", "artist_name", "artist_count", "released_year")
df.show()

# Filter by track_name
df.filter(df['track_name'] == 'Hold My Hand').show()

# Filter operation
df.select("artist_name", "artist_count", "released_year").filter("artist_count = 2").show()

# Stop the SparkSession
spark.stop()