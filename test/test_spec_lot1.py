import os

from get_restitution import get_restitution

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pytest
from pyspark.sql import SparkSession

from get_restitution import get_restitution
from get_restitution2 import get_restitution2
from data_loader import clean_test_data_load
from get_jeu_cle_statique import get_jeu_cle_statique
from pyspark.testing.utils import assertDataFrameEqual

SPEC_LOT1_PATH_EXPECTED = "test/test_spec_lot1_expected/"


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("TestSuite").getOrCreate()


class TestSpecLot1:

    @pytest.fixture(scope="class")
    def test_dataframes(self, spark_session):
        return clean_test_data_load.get_all_df(spark_session=spark_session,
                                               usecase="spec")

    @staticmethod
    def get_context_by_num(spark_session, num_context):
        return clean_test_data_load.get_context_by_num_context(
            spark_session, num_context)

    # Sur une œuvre avec un seul lettrage ou sans lettrage
    # def test_one_or_fewer_lettrage_1(self, spark_session, test_dataframes):
    #     # Demande 1 - Contexte 1
    #     jeu_cle = get_jeu_cle_statique(test_dataframes, [5363801411], self.get_context_by_num(spark_session, 1))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(spark_session,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0001_one_lettrage.csv")
    #
    #     restitution = (get_restitution2(test_dataframes, jeu_cle, self.get_context_by_num(spark_session, 1)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)
    #
    # def test_one_or_fewer_lettrage_5(self, spark_session, test_dataframes):
    #     # Demande 2 - Contexte 5
    #     jeu_cle = get_jeu_cle_statique(test_dataframes, [6785877311], self.get_context_by_num(spark_session, 5))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(spark_session,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0001_no_lettrage.csv")
    #
    #     restitution = (get_restitution(test_dataframes, jeu_cle, self.get_context_by_num(spark_session, 5)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)

    # Sur une œuvre avec plusieurs lettrages
    # @pytest.mark.xfail
    # def test_two_lettrages_one_ad_gestionnaire(self, spark_session, test_dataframes):
    #     # Demande - Contexte 6
    #     jeu_cle = get_jeu_cle_statique(test_dataframes, [3693837711], self.get_context_by_num(spark_session, 6))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(spark_session,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0002_many_lettrages.csv")
    #
    #     restitution = (get_restitution3(test_dataframes, jeu_cle, self.get_context_by_num(spark_session, 6)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)

    # Sur une œuvre avec une branche où aucun ayant droit est gestionnaire
    # @pytest.mark.xfail
    # def test_no_ayant_droit_gestionnaire_on_branch(self, spark_session, test_dataframes):
    #     # Demande - Contexte 1
    #     jeu_cle = get_jeu_cle_statique(test_dataframes, [4794636111], self.get_context_by_num(spark_session, 1))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(spark_session,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0005_no_ad_gestionnaire_on_branch.csv")
    #
    #     restitution = (get_restitution(test_dataframes, jeu_cle, self.get_context_by_num(spark_session, 1)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)

    # Sur une œuvre avec une branche où tous les ayants droit sont gestionnaires
    # def test_all_ad_gestionnaires(self, spark_session, test_dataframes):
    #     # Demande - Contexte 1
    #     jeu_cle = get_jeu_cle_statique(test_dataframes, [2240438311], self.get_context_by_num(spark_session, 1))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(spark_session,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0004_all_ad_gestionnaires.csv")
    #     df_expected_restitution.show()
    #
    #     restitution = (get_restitution3(test_dataframes, jeu_cle, self.get_context_by_num(spark_session, 1)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)

    # https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Avec-plusieurs-contrats-simultan%C3%A9ment-Cas-existant
    # Avec plusieurs contrats simultanément
    @pytest.mark.xfail
    def test_many_contracts_simultaneous(self, spark_session, test_dataframes):
        jeu_cle = get_jeu_cle_statique(test_dataframes, [6785877311],
                                       self.get_context_by_num(spark_session, 1))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(spark_session,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "many_contracts_simultaneous.csv")

        restitution = (get_restitution2(test_dataframes, jeu_cle, self.get_context_by_num(spark_session, 1)))

        assertDataFrameEqual(restitution, df_expected_restitution)
