from pyspark.sql import SparkSession


def csv_to_parquet(csv_file: str):
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.option("header", "true").csv(csv_file)
    df.show()
    df.coalesce(1).write.mode("append").parquet(f"../data/parquet/")
    print("success")


csv_to_parquet("../data/csv/octave_octav1_app_octavt_pcttypdrt.csv")
