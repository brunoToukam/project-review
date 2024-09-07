import pytest
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from data_loader.s3_data_load import get_restitution_data, get_context_by_num_context, get_expected_df_from_csv_file, \
    get_contrat

load_dotenv()


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("TestSuite").getOrCreate()


@pytest.fixture(scope="function")
def test_dataframes(spark_session):
    data_path = "data/setup_usecases/"
    return get_restitution_data(spark=spark_session, data_path=data_path, source="local")


@pytest.fixture(scope="function")
def chaine_contrats(spark_session):
    use_case = "spec"
    depth = 3
    contrat_path = f"data/setup_usecases/{use_case}/computed/contrats/"
    return get_contrat(spark_session, contrat_path, depth)


@pytest.fixture(scope="function")
def get_context_by_num(spark_session):
    def _get_context(num_context):
        return get_context_by_num_context(spark_session, num_context)

    return _get_context


@pytest.fixture(scope="function")
def get_expected_dataframe(spark_session):
    def _get_expected_df(file_path):
        return get_expected_df_from_csv_file(spark_session, file_path)

    return _get_expected_df
