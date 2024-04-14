# Importing sparksession
from pyspark.sql import SparkSession
# Creating a sparksession object
spark = SparkSession.builder.appName("window_function").getOrCreate()

employees_teams = [(1,8),(2,8),(3,8),(4,7),(5,9),(6,9)]

employeeteams = spark.createDataFrame(employees_teams,schema=("employee_id integer, team_id integer"))

employeeteams.show()

employeeteams_new = employeeteams.groupby("team_id").count()
employeeteams_new.show()

employeeteams_new.join(employeeteams,on="team_id", how="inner").select("employee_id", "count").orderBy("employee_id").show()