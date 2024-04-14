# Import the required classes 
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
    .options( url="jdbc:postgresql://localhost:5432/emptdb", # database name 
            dbtable="employees",
            user="postgres",
            password="postgres",
            driver="org.postgresql.Driver") \
    .load()

# Print the Schema
df.printSchema()

# Display the first 20 rows
df.show()

# Create a TempView
df.createOrReplaceTempView("employees")

#  Use SQL to group by fields 
spark.sql(
    """
        select job_id, department_id, round(avg(salary),2) as ave_sal, count(*) 
        from employees
        group by department_id, job_id, manager_id
        having ave_sal > 10000
        order by 3
    """
).show()

#  Use SQL with a subquery
spark.sql(
    """
        select employee_id, first_name, last_name, salary 
        from employees
        where salary < 
        (
        select salary from employees
        where employee_id = 120 
        )
    """
).show()

# Save data to database
# Table employees_sub has been created
# Refer to repository https://github.com/tpitas/data-engineering-with-sql
df.write.mode("overwrite") \
    .format("jdbc") \
    .options( url="jdbc:postgresql://localhost:5432/emptdb", # database name 
        dbtable="employees_sub", # new table created
        user="postgres",
        password="postgres",
        driver="org.postgresql.Driver") \
    .save()

# Stop the SparkSession
spark.stop()