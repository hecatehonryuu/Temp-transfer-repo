import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question3".format(hdfs_nn)

df = spark.read.csv(input_path, header=True, inferSchema=True)

df1 = df.groupBy('City').agg({'Rating': 'avg'})

df2 = df1.sort('avg(Rating)', ascending=False).limit(3).withColumn("RatingGroup", "Top")
df3 = df1.sort('avg(Rating)', ascending=True).limit(3).withColumn("RatingGroup", "Bottom")

df4 = df2.union(df3)

df4.write.csv(output_path)
