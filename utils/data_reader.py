import os
import json
import logging
from typing import Any

from pyspark.sql import SparkSession, DataFrame

import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


class DataReader:
    """
    DataReader class to handle reading data from S3 and RDS.

    Methods
    -------
    get_spark_session():
        Initializes and returns a Spark session.
    configure_spark_for_s3(spark):
        Configures Spark to access AWS S3.
    read_data(data_key, select_columns=True):
        Reads data from a parquet file in S3.
    read_from_db(table_key, select_columns=True):
        Reads data from a PostgreSQL table.
    stop_spark_session():
        Stops the Spark session.
    """

    def __init__(self):
        self.spark = self.get_spark_session()
        self.configure_spark_for_s3(self.spark)
        self.data_dictionary = self.load_data_dictionary()

    @staticmethod
    def load_data_dictionary() -> dict:
        """
        Load the data dictionary from a JSON file.

        Returns
        -------
        dict
            The data dictionary loaded from the JSON file.
        """
        try:
            with open('chaine_contrat_dictionary.json') as f:
                data_dictionary = json.load(f)
            return data_dictionary
        except FileNotFoundError:
            logger.error("Data dictionary file not found.")
            return {}

    @staticmethod
    def get_spark_session() -> SparkSession:
        """
        Initialize and return a SparkSession.

        Returns
        -------
        SparkSession
            The Spark session configured with necessary packages and options.
        """
        try:
            spark = (
                SparkSession.builder.appName("perfo_v2")
                .config("spark.jars.packages",
                        "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
                .config("spark.jars", settings.JDBC_DRIVER_PATH)
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
                .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:utils/log4j.properties")
                .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:utils/log4j.properties")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            logger.info("SparkSession created successfully.")
            return spark
        except Exception as e:
            logger.error(f"Error creating SparkSession: {e}")
            raise

    @staticmethod
    def configure_spark_for_s3(spark: SparkSession) -> None:
        """
        Configure Spark to access AWS S3.

        Parameters
        ----------
        spark : SparkSession
            The Spark session to be configured for S3 access.
        """
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            hadoop_conf.set("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
            hadoop_conf.set("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
            hadoop_conf.set("spark.hadoop.fs.s3a.session.token", settings.AWS_SESSION_TOKEN)
            hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
            hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
            hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
            hadoop_conf.set("fs.s3a.path.style.access", "true")
            logger.info("Spark configured for S3 access.")
        except Exception as e:
            logger.error(f"Error configuring Spark for S3: {e}")
            raise

    @staticmethod
    def alias_columns(dataframe: DataFrame, alias_prefix: str) -> DataFrame:
        """
        Alias all columns in a DataFrame with a given prefix.

        Parameters
        ----------
        dataframe : DataFrame
            The Spark DataFrame to alias.
        alias_prefix : str
            The prefix to add to each column name.

        Returns
        -------
        DataFrame
            The aliased Spark DataFrame.
        """
        for column in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(column, f"{alias_prefix}_{column}")
        return dataframe

    def read_data(self, data_key: str, all_columns: bool = False, local: bool = True,
                  file_type: str = 'parquet') -> DataFrame:
        """
        Read data from a parquet file in S3 using the data dictionary.

        Parameters
        ----------
        data_key : str
            The key for the data entry in the data dictionary.
        all_columns : bool, optional
            Whether to select all or only the columns mentioned in the data dictionary, by default False.
        local: bool, optional
            Whether to read the data stored locally or stored in s3, by default True
        file_type: str
            We could sometimes read csv files

        Returns
        -------
        DataFrame
            The Spark DataFrame containing the data from the parquet file.
        """
        try:
            data_info: dict[str, Any] = self.data_dictionary.get(data_key, {})
            path: str = data_info.get("local_path", "")
            columns: list[str] = data_info.get("columns", [])

            if not local:
                path: str = data_info.get("s3_path", "")

            if not path:
                raise ValueError(f"No path found for data key: {data_key}")

            logger.info(f"Reading data from {path}...")
            if file_type == "csv":
                dataframe: DataFrame = self.spark.read.option("header", "true").option('inferSchema', 'true').csv(path)
            else:
                dataframe: DataFrame = self.spark.read.option("header", "true").option('inferSchema', 'true').parquet(
                    path)

            if columns and not all_columns:
                dataframe = dataframe.select(columns)

            # dataframe = self.alias_columns(dataframe, data_key)
            logger.info("Data read successfully.")
            return dataframe.drop_duplicates()  # dataframe.drop_duplicates(subset=["column1", "column2"])
        except Exception as e:
            logger.error(f"Error reading data from {path}: {e}")
            raise

    def read_from_db(self, table_key: str, all_columns: bool = False) -> DataFrame:
        """
        Read data from a PostgreSQL table using the data dictionary.

        Parameters
        ----------
        table_key : str
            The key for the table entry in the data dictionary.
        all_columns : bool, optional
            Whether to select all or only the columns mentioned in the data dictionary, by default False.

        Returns
        -------
        DataFrame
            The Spark DataFrame containing the data from the PostgreSQL table.
        """
        try:
            table_info = self.data_dictionary.get(table_key, {})
            columns: list[str] | None = table_info.get("columns", [])
            table_name: str = table_info.get("table_name", "")

            jdbc_user = os.environ.get("JDBC_USER")
            jdbc_pwd = os.environ.get("JDBC_PWD")
            jdbc_url = os.getenv("JDBC_URL")

            logger.debug(f"JDBC URL: {jdbc_url}")
            logger.debug(f"JDBC User: {jdbc_user}")

            if not all([jdbc_user, jdbc_pwd, jdbc_url]):
                raise ValueError("JDBC configuration is incomplete. Check environment variables.")

            jdbc_prop = {
                "user": jdbc_user,
                "password": jdbc_pwd,
                "driver": "org.postgresql.Driver"
            }

            logger.info(f"Reading data from table {table_name}...")
            dataframe: DataFrame = self.spark.read.jdbc(jdbc_url, table_name, properties=jdbc_prop)

            if columns and not all_columns:
                dataframe = dataframe.select(columns)

            logger.info("Data read successfully.")
            return dataframe.drop_duplicates()
        except Exception as e:
            logger.error(f"Error reading data from table {table_name}: {e}")
            raise

    def read_all_data(self, all_columns: bool = False, local: bool = True) -> dict[str, DataFrame]:
        """
        Read all data sources specified in the data dictionary.

        Parameters
        ----------
        all_columns : bool, optional
            Whether to select all or only the columns mentioned in the data dictionary, by default False.
        local: bool, optional
            Whether to read the data stored locally or stored in s3, by default True

        Returns
        -------
        Dict[str, DataFrame]
            A dictionary containing the Spark DataFrames for all data sources.
        """
        data_frames = {}
        for data_key in self.data_dictionary.keys():
            if 'local_path' in self.data_dictionary[data_key]:  # It's a file-based data source
                df = self.read_data(data_key, all_columns=all_columns, local=local)
            else:  # It's a database table
                df = self.read_from_db(data_key, all_columns=all_columns)
            data_frames[data_key] = df
        return data_frames

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context related to this object.
        """
        self.stop_spark_session()

    def stop_spark_session(self) -> None:
        """
        Stop the SparkSession.
        """
        try:
            self.spark.stop()
            logger.info("SparkSession stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping SparkSession...: {e}")
            raise

    def read_local(self, path):
        return self.spark.read.parquet(path)


# Example usage
if __name__ == "__main__":
    exit()
