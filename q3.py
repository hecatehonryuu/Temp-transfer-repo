import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question3".format(hdfs_nn)

sc = spark.sparkContext
text_file = sc.textFile(input_path)

# Aggregate by average of column 5 using column 2 as key
city_rating = text_file.map(lambda line: line.split(",")).map(lambda x: (x[2], float(x[5])))

average_rating = city_rating.aggregateByKey((0, 0), lambda acc, value: (acc[0] + value, acc[1] + 1), lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])).mapValues(lambda x: x[0] / x[1])

sorted_rating = average_rating.sortBy(lambda x: -x[1])

sorted_rating_count = sorted_rating.count()
filtered_rating = sorted_rating.zipWithIndex().filter(lambda x: x[1] < 3 or x[1] >= sorted_rating_count - 3).map(lambda x: (x[0][0], x[0][1], "Top" if x[1] < 3 else "Bottom"))

headers = sc.parallelize([("City", "AverageRating", "RatingGroup")])
result = headers.union(filtered_rating)

# Save the result to the output path
result.saveAsTextFile(output_path)
