import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question4".format(hdfs_nn)

sc = spark.sparkContext
text_file = sc.textFile(input_path)

# Aggregate by count using column 2 and 3 as key
city_cuisine = text_file.map(lambda line: line.split(",")).map(lambda x: (x[2], x[3], 1))

count = city_cuisine.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], x[0][1], x[1]))

headers = sc.parallelize([("City", "Cuisine", "count")])
result = headers.union(count)

# Save the result to the output path
result.saveAsTextFile(output_path)