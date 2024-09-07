from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, array_intersect, array_distinct, array_union, sort_array, collect_list, coalesce, array, size
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField

# Create a Spark session
spark = SparkSession.builder.appName("ChainContracts").getOrCreate()

# Sample data
data = [
    (1, [1], [2, 3]),
    (2, [3], [4]),
    (3, [7, 8], [5]),
    (4, [6], [12, 13]),
    (5, [13], [14]),
    (6, [5], [17]),
    (7, [4], [20])
]

# Define the schema
schema = StructType([
    StructField("chaine_contrat", IntegerType(), False),
    StructField("numcatal_cedant", ArrayType(IntegerType()), False),
    StructField("numcatal_cess", ArrayType(IntegerType()), False)
])

# Create the DataFrame
df = spark.createDataFrame(data, schema)


# Function to find chains
def find_chains(df):
    # Initialize chained columns
    df = df.withColumn("chained_ctr", array(col("chaine_contrat"))) \
        .withColumn("chained_cedant", col("numcatal_cedant")) \
        .withColumn("chained_cess", col("numcatal_cess"))

    new_links_found = True

    while new_links_found:
        new_links_found = False

        # Self-join to find links
        joined_df = df.alias("a").join(
            df.alias("b"),
            size(array_intersect(col("a.chained_cess"), col("b.numcatal_cedant"))) > 0
        )

        # Update chained columns
        updated_df = joined_df.select(
            col("a.*"),
            array_distinct(sort_array(array_union("a.chained_ctr", array("b.chaine_contrat")))).alias(
                "new_chained_ctr"),
            col("b.numcatal_cess").alias("new_chained_cess")
        )

        # Check for new links
        changed_rows = updated_df.filter(
            (col("new_chained_ctr") != col("chained_ctr")) |
            (col("new_chained_cess") != col("chained_cess"))
        )

        if changed_rows.count() > 0:
            new_links_found = True

            # Update the main DataFrame
            df = df.join(changed_rows.select("chaine_contrat", "new_chained_ctr", "new_chained_cess"), "chaine_contrat",
                         "left_outer") \
                .withColumn("chained_ctr", coalesce(col("new_chained_ctr"), col("chained_ctr"))) \
                .withColumn("chained_cess", coalesce(col("new_chained_cess"), col("chained_cess"))) \
                .drop("new_chained_ctr", "new_chained_cess")

    return df.select("chained_ctr", "chained_cedant", "chained_cess")


# Find the chains
result_df = find_chains(df)

# Show the result
result_df.show(truncate=False)