import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question1".format(hdfs_nn)

df = spark.read.csv(input_path, header=True, inferSchema=True)

df = df.filter((df['Number of Reviews'].isNotNull()) & (df['Rating'].isNotNull()))

df = df.filter((df['Number of Reviews'] > 0) & (df['Rating'] >= 1.0))

df.write.csv(output_path)
