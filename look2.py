from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_intersect, array_distinct, array_union, sort_array, size, slice

# Create a Spark session
spark = SparkSession.builder.appName("ChainContracts").getOrCreate()

# Sample data
data = [
    ([1], [1], [2, 3]),
    ([2], [3], [4]),
    ([3], [7, 8], [5]),
    ([4], [6], [12, 13]),
    ([5], [13], [14]),
    ([6], [5], [17]),
    ([7], [4], [20]),
    ([8], [20], [21])
]

# Define the schema
schema = ["chained_ctr", "numcatal_cedant", "numcatal_cess"]

# Create the DataFrame
df = spark.createDataFrame(data, schema)
final_df = df
print("Initial DataFrame:")
df.show(truncate=False)

for i in range(3):
    # Perform the first join
    joined_df = df.alias("a").join(
        df.alias("b"),
        size(array_intersect(col("a.numcatal_cess"), col("b.numcatal_cedant"))) > 0
    )

    # Update chained columns
    df = joined_df.select(
        array_distinct(sort_array(array_union("a.chained_ctr", "b.chained_ctr"))).alias("chained_ctr"),
        array_distinct(array_union("a.numcatal_cedant", "b.numcatal_cedant")).alias("numcatal_cedant"),
        "b.numcatal_cess"
    ).drop_duplicates()

    print("Updated DataFrame after first join:")
    df.show(truncate=False)

    df = df.withColumn("numcatal_cedant", slice(col("numcatal_cedant"), 1, size("numcatal_cedant") - 1))
    print('after slice')
    df.show()

    final_df = final_df.unionByName(df)

print("================= final df ===============")
final_df.show()


"""

================= final df ===============
+------------+---------------+-------------+
| chained_ctr|numcatal_cedant|numcatal_cess|
+------------+---------------+-------------+
|         [1]|            [1]|       [2, 3]|
|         [2]|            [3]|          [4]|
|         [3]|         [7, 8]|          [5]|
|         [4]|            [6]|     [12, 13]|
|         [5]|           [13]|         [14]|
|         [6]|            [5]|         [17]|
|         [7]|            [4]|         [20]|
|         [8]|           [20]|         [21]|
|      [1, 2]|            [1]|          [4]|
|      [2, 7]|            [3]|         [20]|
|      [3, 6]|         [7, 8]|         [17]|
|      [4, 5]|            [6]|         [14]|
|      [7, 8]|            [4]|         [21]|
|[1, 2, 7, 8]|            [1]|         [21]|
+------------+---------------+-------------+
"""