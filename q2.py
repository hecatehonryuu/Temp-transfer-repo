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

df = df.filter((df['Price Range'].isNotNull()))

# Convert DataFrame to RDD
rdd = df.rdd

# Map each row to a key-value pair, where the key is ('City', 'Price Range') and the value is the entire row
rdd_keyed = rdd.map(lambda row: ((row['City'], row['Price Range']), row))

# Reduce by key to find the row with the max and min 'Rating' for each key
max_rdd = rdd_keyed.reduceByKey(lambda a, b: a if a['Rating'] > b['Rating'] else b)
min_rdd = rdd_keyed.reduceByKey(lambda a, b: a if a['Rating'] < b['Rating'] else b)

# Union max_rdd and min_rdd
result_rdd = max_rdd.union(min_rdd)

# Remove the keys to get back the original rows
result_rdd = result_rdd.map(lambda x: x[1])

# Convert the result RDD back to a DataFrame
result_df = result_rdd.toDF()

# Join the original DataFrame with the result DataFrame
df_filtered = df.join(result_df, ['City', 'Price Range', 'Rating'])

# Write the filtered DataFrame to a CSV file
df_filtered.write.csv(output_path)



# df1 = df.groupBy('City', 'Price Range').agg({'Rating': 'max'})
# df2 = df.groupBy('City', 'Price Range').agg({'Rating': 'min'})

# df3 = df1.union(df2)

# df3.write.csv(output_path)
