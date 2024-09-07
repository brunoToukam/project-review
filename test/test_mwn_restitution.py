import pytest
from pyspark.sql import SparkSession

from get_restitution import get_restitution
from data_loader import clean_test_data_load
from get_jeu_cle_statique import get_jeu_cle_statique
from pyspark.testing.utils import assertDataFrameEqual

MWN_PATH_EXPECTED = "test/test_mwn_expected/"


@pytest.fixture(scope="session")
def sparkSession():
    return SparkSession.builder.config("spark.driver.memory", "7g").appName("TestSuite").getOrCreate()


class TestMWNRestitution:

    @pytest.fixture(scope="class")
    def testDataframes(self, sparkSession):
        return clean_test_data_load.get_all_df(spark_session=sparkSession,
                                               usecase="mwn",
                                               source="local")

    def test_hello_france_now_DE(self, sparkSession, testDataframes):
        # Demande
        context = {
            "code_iso4n_pays": 250,
            "code_famille_type_utilisation": "ONLINE",
            "code_type_utilisation": "STREAMING",
            "code_type_droit": "DE",
            "date_consultation": "2024-06-21",
            "date_effet": "2024-06-21"
        }

        list_code_oeuvres = [2974081211]

        # Recuperation du jeu de cle statique
        jeu_cle = get_jeu_cle_statique(testDataframes, list_code_oeuvres, context)

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     MWN_PATH_EXPECTED + "hello_FR_now_DE.csv")

        # Restitution
        restitution = (get_restitution(testDataframes, jeu_cle, context))

        assertDataFrameEqual(restitution, df_expected_restitution)

    #def test_candy_necklace_UK_now_DE(self, sparkSession, testDataframes):
    #    # Demande
    #    context = {
    #        "code_iso4n_pays": 826,
    #        "code_famille_type_utilisation": "ONLINE",
    #        "code_type_utilisation": "STREAMING",
    #        "code_type_droit": "DE",
    #        "date_consultation": "2024-06-21",
    #        "date_effet": "2024-06-21"
    #    }
#
    #    list_code_oeuvres = [5239458111]
#
    #    jeu_cle = get_jeu_cle_statique(testDataframes, list_code_oeuvres, context)

    #    # Expected
    #    df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
    #                                                                                 MWN_PATH_EXPECTED + "candy_UK_now_DE.csv")
#
    #    restitution = (get_restitution(testDataframes, jeu_cle, context))
#
    #    assertDataFrameEqual(restitution, df_expected_restitution)
#
    #def test_fortnight_france_now_DE(self, sparkSession, testDataframes):
    #    # Demande
    #    context = {
    #        "code_iso4n_pays": 250,
    #        "code_famille_type_utilisation": "ONLINE",
    #        "code_type_utilisation": "STREAMING",
    #        "code_type_droit": "DE",
    #        "date_consultation": "2024-06-21",
    #        "date_effet": "2024-06-21"
    #    }
#
    #    list_code_oeuvres = [111015968567]
#
    #    jeu_cle = get_jeu_cle_statique(testDataframes, list_code_oeuvres, context)
#
    #    # Expected
    #    df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
    #                                                                                 MWN_PATH_EXPECTED + "fortnight_FR_now_DE.csv")
#
    #    restitution = (get_restitution(testDataframes, jeu_cle, context))
#
    #    assertDataFrameEqual(restitution, df_expected_restitution)
