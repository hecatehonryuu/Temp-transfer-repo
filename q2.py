import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question2".format(hdfs_nn)

df = spark.read.csv(input_path, header=True, inferSchema=True)

df = df.filter((df['Price Range'].isNotNull()) & (df['Rating'].isNotNull()))

df1 = df.groupBy('City', 'Price Range').agg({'Rating': 'max'})
df2 = df.groupBy('City', 'Price Range').agg({'Rating': 'min'})

df3 = df1.union(df2)

df4 = df3.join(df, ['City', 'Price Range'])

df4.write.csv(output_path)

