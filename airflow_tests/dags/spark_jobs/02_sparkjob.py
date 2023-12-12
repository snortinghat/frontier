from pyspark.sql import SparkSession
# import time

spark = SparkSession.builder.appName("PySparkTest_2").getOrCreate()

# Create a simple DataFrame
data = [("Alice", 10), ("Bob", 20), ("Charlie", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame and print some information
print("DataFrame Contents:")
df.show()

# time.sleep(30)

# Perform some basic operations
print("Average Age:")
df.selectExpr("avg(Age)").show()

# # Perform some basic operations
# print("Average Age:")
# df.selectExpr("avg(Age)").show()

# "/user/airblast/etl_test/folder"


# "/tmp/folder/"

# Your PySpark application logic here

spark.stop()