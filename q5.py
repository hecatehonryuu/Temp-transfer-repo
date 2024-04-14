import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://{}:9000/assignment2/part2/input/tmdb_5000_credits.parquet".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question5".format(hdfs_nn)

df = spark.read.option("header", "true").parquet(input_path)

df = df.select("movie_id", "cast")

df.write.csv(output_path)

spark.stop()
