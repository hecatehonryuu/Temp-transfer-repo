import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question2".format(hdfs_nn)

sc = spark.sparkContext
text_file = sc.textFile(input_path)

#Filter out null price range
text_file = text_file.filter(lambda row: row.split(',')[6] != 'null')

# Group by city and price range
grouped_values = text_file.map(lambda row: (row.split(',')[2], row.split(',')[6], float(row.split(',')[5]))).groupBy(lambda x: (x[0], x[1]))

#Find max and min values
max_values = grouped_values.reduceByKey(lambda x, y: x if x[1][2] > y[1][2] else y)
min_values = grouped_values.reduceByKey(lambda x, y: x if x[1][2] < y[1][2] else y)

#filter to leave only min/max values
filtered_text_file = text_file.filter(lambda row: (row.split(',')[2], row.split(',')[6]) in min_values or (row.split(',')[2], row.split(',')[6]) in max_values)

filtered_text_file.saveAsTextFile(output_path)
