from pyspark.sql import SparkSession
# import time

spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

# Create a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame and print some information
print("DataFrame Contents:")
df.show()

# time.sleep(30)

# Perform some basic operations
print("Average Age:")
df.selectExpr("avg(Age)").show()

# Your PySpark application logic here

spark.stop()