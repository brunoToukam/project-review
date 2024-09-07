import json
import logging
import os
from datetime import datetime

import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from generate_ter import get_territories_computed, get_final_territories_by_idtercmplx

# Initialize logging
logger = logging.getLogger(__name__)

SCHEMA_PATH = {
    "input": "data_loader/schema/schemas_det_contrat.json",
    "restit": "data_loader/schema/schemas_restitution.json",
    "contexte": "data_loader/schema/schemas_contexte.json",
    "expected": "data_loader/schema/schema_expected.json"
}


def construct_schema(columns: dict[str, str]) -> T.StructType:
    type_mapping = {
        "integer": (T.IntegerType(), True),
        "long": (T.LongType(), True),
        "string": (T.StringType(), True),
        "double": (T.DoubleType(), True),
        "float": (T.FloatType(), True),
        "date": (T.DateType(), True),
        "bool": (T.BooleanType(), True),
        "array": (T.ArrayType(T.IntegerType()), True),
        "string_optional": (T.StringType(), False),
        "array_string": (T.ArrayType(T.StringType()), True),
        "array_long": (T.ArrayType(T.LongType()), True)
    }

    schema = T.StructType()
    for col_name, col_type in columns.items():
        schema = schema.add(T.StructField(col_name, *type_mapping[col_type]))
    return schema


def read_data(spark: SparkSession, file_path: str, schema_cols: dict[str, str] = None, source="remote") -> DataFrame:
    if source == "local":
        df = spark.read.csv(f"{file_path}.csv", header=True, schema=construct_schema(schema_cols))
    else:
        df = spark.read.parquet(f"{file_path}/")

    logger.info(f"{file_path} read successfully!")

    return df


def write_df(df: DataFrame, s3_path: str, table_name: str) -> None:
    logger.info(f"Writing {table_name} to {s3_path}...")
    if table_name == "contrat":
        df.write.format('parquet').partitionBy("cdetypdrtsacem").mode("overwrite").save(s3_path)
    else:
        df.write.format('parquet').mode("overwrite").save(s3_path)

    logger.info(f"{table_name} successfully saved to {s3_path}")


def build_territories(spark, all_df, data_path, schemas, source, bucket) -> dict[str, DataFrame]:
    bucket = None if source == "local" else bucket
    check_path = data_path if source == "local" else "contrat_replication/"

    for table in ["territories", "territories_by_id_tercmplx"]:
        table_path = data_path + f"computed/{table}"
        logger.info(f"Reading data from {table_path}...")

        try:
            if not is_empty(bucket, f"{check_path}computed/{table}/"):
                all_df[table] = read_data(spark=spark,
                                          file_path=table_path,
                                          schema_cols=schemas["computed"][table]
                                          ).alias(f"df_{table}")
                logger.info(f"{table} successfully read from {table_path}")
            else:
                if table == "territories":
                    all_df[table] = get_territories_computed(all_df["gpeter"]).alias(f"df_{table}")
                else:
                    all_df[table] = get_final_territories_by_idtercmplx(
                        all_df["eltter"], all_df["territories"]).alias(f"df_{table}")
                logger.info(f"{table} successfully computed")
                write_df(df=all_df[table], table_name=table, s3_path=table_path)
        except Exception as e:
            logger.error(f"Failed to read table {table}: {e}")

    return all_df


def get_all_df(spark_session: SparkSession, data_path: str, source="remote", use_case="spec") -> dict[str, DataFrame]:
    bucket = data_path.split("/")[2]
    base_output = f"output/contrats_base/contrat_base_{datetime.now().strftime('%Y-%m-%d')}/"
    if source == "remote" and not is_empty(bucket, base_output):
        return {"contrat_hierarchy_base": read_data(spark_session, f"s3a://{bucket}/{base_output}")}
    else:
        all_df = {}
        if source == "local":
            data_path += f"{use_case}/"

        logger.info("Creating dataframes for PERFO: Determination des chaines de contrats...")
        with open(SCHEMA_PATH.get("input"), 'r') as f:
            schemas = json.loads(f.read())

            for df_name in schemas.keys():
                if df_name != "computed":
                    all_df[df_name] = read_data(spark=spark_session,
                                                file_path="{}{}/{}".format(data_path,
                                                                           schemas[df_name].get("rds_schema"),
                                                                           schemas[df_name].get("rds_table_name")),
                                                schema_cols=schemas[df_name].get("attributes"),
                                                source=source).alias(f"df_{df_name}")
                else:
                    all_df = build_territories(spark_session, all_df, data_path, schemas, source, bucket)

        logger.info("LOADING FROM S3 OK!")
        return all_df


def is_empty(s3_bucket: str, folder: str) -> bool:
    if s3_bucket is None:
        return not os.path.exists(folder)
    else:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=folder)
        return 'Contents' not in response


def get_contrat(spark: SparkSession, path, depth, date_consultation: str = None):
    if not date_consultation:
        date_consultation = datetime.now().strftime("%Y-%m-%d")

    path = f"{path}contrat_profondeur_{depth}_{date_consultation}/"

    return read_data(spark, path)


def get_expected_df_from_csv_file(spark_session, file_path):
    with open(SCHEMA_PATH.get("expected"), 'r') as f:
        schemas = json.loads(f.read())
        df = spark_session.read.csv(file_path, schema=construct_schema(schemas["expected"]), header=True)

        # transform column chaine_contrats to an array of integer splitting on pipe
        df = (df.withColumn("chaine_contrats", F.split(df["chaine_contrats"], "\\|"))
              .withColumn("chaine_contrats", F.expr("transform(chaine_contrats, x -> cast(x as int))"))
              .withColumn("cle", F.col("cle").cast(T.DoubleType()))
              # .withColumn("cle", round(col("cle"), 2))
              )

    return df


def get_context_by_num_context(spark_session, num_context: int):
    file_path = "test/test_contextes/contextes.csv"
    with open(SCHEMA_PATH.get("contexte"), 'r') as f:
        schemas = json.loads(f.read())
        df = spark_session.read.csv(file_path, schema=construct_schema(schemas[f"contexte"]), header=True)

    return df.filter(df.num_contexte == num_context).first()


def get_restitution_data(spark: SparkSession, data_path: str, source="remote", use_case="spec") -> dict[str, DataFrame]:
    bucket = data_path.split("/")[2]
    all_df = {}
    if source == "local":
        data_path += f"{use_case}/"
    with open(SCHEMA_PATH.get("restit"), 'r') as f:
        schemas = json.loads(f.read())
        for df_name in schemas.keys():
            if df_name != "computed":
                file_path = f"{data_path}{schemas[df_name].get('rds_schema')}/{schemas[df_name].get('rds_table_name')}"
                all_df[df_name] = read_data(spark, file_path, schemas[df_name].get("attributes"),
                                            source).drop_duplicates()
            else:
                all_df = build_territories(spark, all_df, data_path, schemas, source, bucket)

    logger.info("Data for restitution loaded!")

    return all_df
