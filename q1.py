import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question1".format(hdfs_nn)

sc = spark.sparkContext
text_file = sc.textFile(input_path)

filtered_text_file = text_file.filter(lambda row: row.split(',')[7] != 'null' and row.split(',')[5] != 'null' and float(row.split(',')[5]) >= 1.0)

filtered_text_file.saveAsTextFile(output_path)
