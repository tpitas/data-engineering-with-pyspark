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
df1 = spark.read.format("jdbc") \
    .options( url="jdbc:postgresql://localhost:5432/retail_db",
             dbtable="orders",
             user="postgres",
             password="postgres",
             driver="org.postgresql.Driver") \
    .load()

# Print the Schema
df1.printSchema()

# Create a TempView
df1.createOrReplaceTempView("orders")

# Use SQL to select the first 20 rows
spark.sql("select * from orders").show()

# Create a DataFrame
df2 = spark.read.format("jdbc") \
    .options( url="jdbc:postgresql://localhost:5432/retail_db",
             dbtable="order_items",
             user="postgres",
             password="postgres",
             driver="org.postgresql.Driver") \
    .load()

# Print the Schema
df2.printSchema()

# Create a TempView
df1.createOrReplaceTempView("order_items")

# Use SQL to select the first 20 rows
spark.sql("select * from order_items").show()

# Join the two tables 
spark.sql(
    """select oe.order_id, to_date(oe.order_date,'MON-DD-YYYY') as date, oi.order_status
        from orders as oe
        inner join order_items as oi
        on oe.order_id = oi.order_id
        where oi.order_status = 'PENDING'
    """).show()

# Stop the SparkSession
spark.stop()
