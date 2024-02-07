# Import the required classes
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create a SparkSession 
spark = SparkSession.builder.appName("Customers").getOrCreate()

schema = StructType([
   StructField('CustomerID', IntegerType(), False),
   StructField('LastName',   StringType(),  False),
   StructField('FirstName',  StringType(),  False),
   StructField('CustomerEmail',  StringType(),  False),
   StructField('CustomerStreet',  StringType(),  False),
   StructField('CustomerCity',  StringType(),  False),
   StructField('CustomerState',   StringType(),  False),
   StructField('CustomerZipcode',   StringType(),  False)
])

# Create a DataFrame
data = [
        [ 1 , 'Richard' , 'Hernandez', 'Richard.Hernandez@example.com','6303 Heather Plaza', 'Brownsville', 'TX' , '78521' ], 
        [ 2 , 'Mary' , 'Barrett', 'Mary.Barrett@example.com','9526 Noble Embers Ridge', 'Littleton  ', 'CO' , '80126' ], 
        [ 3 , 'Ann' , 'Smith', 'Ann.Smith@example.com','3422 Blue Pioneer Bend', 'Caguas', 'PR' , '00725' ], 
        [ 4 , 'Mary' , 'Jones', 'Mary.Jones@example.com','8324 Little Common', 'San Marcos', 'CA' , '92069' ], 
        [ 5 , 'Robert' , 'Hudson', 'Robert.Hudson@example.com','10 Crystal River Mall', 'Caguas', 'PR' , '00725' ]
     ]

customers = spark.createDataFrame(data, schema)
customers.show()


