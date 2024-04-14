from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("nyc taxi") \
    .config("spark.executor.memory", '4g') \
    .config("spark.executor.cores", '2') \
    .config("spark.cores.max", '2') \
    .getOrCreate()

df = spark.read.parquet('/home/barry/Desktop/yellow_tripdata_2023-11.parquet')
df.printSchema()


