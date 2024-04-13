import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

input_path = "hdfs://{}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question2".format(hdfs_nn)
output_path1 = "hdfs://{}:9000/assignment2/output/question2/1".format(hdfs_nn)
output_path2 = "hdfs://{}:9000/assignment2/output/question2/2".format(hdfs_nn)
output_path3 = "hdfs://{}:9000/assignment2/output/question2/3".format(hdfs_nn)
output_path4 = "hdfs://{}:9000/assignment2/output/question2/4".format(hdfs_nn)

df = spark.read.csv(input_path, header=True, inferSchema=True)

df = df.filter((df['Price Range'].isNotNull()))

df.write.csv(output_path1)

df1 = df.groupBy('City', 'Price Range').agg({'Rating': 'max'})
df.write.csv(output_path2)
df2 = df.groupBy('City', 'Price Range').agg({'Rating': 'min'})
df.write.csv(output_path3)

df = df.filter(df['City'].isin((df1.select('City')) & df['Price Range'].isin(df1.select('Price Range'))) | (df['City'].isin(df2.select('City')) & df['Price Range'].isin(df2.select('Price Range'))))

df.write.csv(output_path4)

# sc = spark.sparkContext
# text_file = sc.textFile(input_path)

# #Filter out null price range
# text_file = text_file.filter(lambda row: row.split(',')[6] != 'null')

# # Group by city and price range
# grouped_values = text_file.map(lambda row: (row.split(',')[2], row.split(',')[6], float(row.split(',')[5])))

# #Find max and min values
# max_values = grouped_values.reduceByKey(max)
# min_values = grouped_values.reduceByKey(min)

# max_values_set = set(max_values.collect())
# min_values_set = set(min_values.collect())

# #filter to leave only min/max values
# filtered_text_file = text_file.filter(lambda row: ((row.split(',')[2], row.split(',')[6]), float(row.split(',')[5])) in max_values_set or ((row.split(',')[2], row.split(',')[6]), float(row.split(',')[5])) in min_values_set)

# filtered_text_file.saveAsTextFile(output_path)
