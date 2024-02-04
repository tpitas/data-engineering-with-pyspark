# Import the required classes assuming PySpark has been installed
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

#  Create a new configuration
conf = SparkConf()
conf.set('spark.jars','/home/ansible/Downloads/postgresql-42.4.2.jar') # driver downloaded from https://jdbc.postgresql.org/download/

# Create a SparkSession 
spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local") \
        .appName("Postgres Connection Test") \
        .getOrCreate()

# Create a DataFrame
df = spark.read.format("jdbc") \
    .options( url="jdbc:postgresql://localhost:5432/retail_db",
             dbtable="categories",
             user="postgres",
             password="postgres",
             driver="org.postgresql.Driver") \
    .load()

# Print the Schema
df.printSchema()

# Creating a TempView

df.createOrReplaceTempView("categories")

#  Use SQL to filter data and select the first 20 rows
spark.sql("select * from categories").show()

# Filter the data
category_name = spark.sql(
    """select * from categories where 
    category_name like '%ball%'
    """)
category_name.show()

# Stop the SparkSession
spark.stop()