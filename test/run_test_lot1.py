import pytest


@pytest.mark.usefixtures("spark_session", "test_dataframes", "get_context_by_num", "get_expected_dataframe",
                         "chaine_contrats")
class RunTestSpecLot1:
    @pytest.fixture(scope="function", autouse=True)
    def setup_class(self, request, spark_session, test_dataframes, get_context_by_num, get_expected_dataframe,
                    chaine_contrats):
        print("Setting up class in RunTestSpecLot1")
        self.spark_session = spark_session
        self.test_dataframes = test_dataframes
        self.chaine_contrats = chaine_contrats
        self.get_context_by_num = get_context_by_num
        self.get_expected_dataframe = get_expected_dataframe
