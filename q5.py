import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import explode
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
input_path = "hdfs://{}:9000/assignment2/part2/input/tmdb_5000_credits.parquet".format(hdfs_nn)
output_path = "hdfs://{}:9000/assignment2/output/question5".format(hdfs_nn)
output_path1 = "hdfs://{}:9000/assignment2/output/question5/1".format(hdfs_nn)
output_path2 = "hdfs://{}:9000/assignment2/output/question5/2".format(hdfs_nn)
output_path3 = "hdfs://{}:9000/assignment2/output/question5/3".format(hdfs_nn)

df = spark.read.option("header", "true").parquet(input_path)

df = df.select("movie_id", "cast")

json_schema = "array<struct<cast_id:int, character:string, credit_id:string, gender:int, id:int, name:string, order:int>>"
df = df.withColumn("cast", explode(from_json(col("cast"), json_schema)))
df = df.select("movie_id", "cast.cast_id", "cast.character", "cast.credit_id", "cast.gender", "cast.id", "cast.name", "cast.order")

df.write.csv(output_path1)
df = df.select("movie_id", "cast.gender", "cast.name")

df.write.csv(output_path2)

spark.stop()
