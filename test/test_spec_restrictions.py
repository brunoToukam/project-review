import os

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pytest
from pyspark.sql import SparkSession

from get_restitution import get_restitution
from data_loader import clean_test_data_load
from get_jeu_cle_statique import get_jeu_cle_statique
from pyspark.testing.utils import assertDataFrameEqual

SPEC_LOT1_PATH_EXPECTED = "test/test_spec_lot1_expected/"


@pytest.fixture(scope="session")
def sparkSession():
    return SparkSession.builder.appName("TestSuite").getOrCreate()


class TestSpecRestrictions:

    @pytest.fixture(scope="class")
    def testDataframes(self, sparkSession):
        return clean_test_data_load.get_all_df(spark_session=sparkSession,
                                               usecase="spec",
                                               recalculate_computed_dataframes=False)

    @staticmethod
    def get_context_by_num(spark_session, num_context):
        return clean_test_data_load.get_context_by_num_context(
            spark_session, num_context)

    # @pytest.mark.xfail
    def test_restr_person_non_strict_inclusive(self, sparkSession, testDataframes):
        # Demande - Contexte 1
        jeu_cle = get_jeu_cle_statique(testDataframes,
                                       [4174113611, 3536315501],
                                       self.get_context_by_num(sparkSession, 1))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_person_non_strict_inclusive.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 1)))
        restitution.show()

        assertDataFrameEqual(restitution, df_expected_restitution)

    # Sur les personnes de type stricte inclusive
    # @pytest.mark.xfail
    def test_restr_person_strict_inclusive_contrat_KO(self, sparkSession, testDataframes):
        # Demande - Contexte 2
        jeu_cle = get_jeu_cle_statique(testDataframes, [4024962211], self.get_context_by_num(sparkSession, 2))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_person_strict_inclusive_contrat_KO.csv")
        df_expected_restitution.show()

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 2)))
        restitution.show()

        assertDataFrameEqual(restitution, df_expected_restitution)

    # @pytest.mark.xfail
    # def test_restr_person_strict_inclusive_contrat_OK_1(self, sparkSession, testDataframes):
    #     # Demande - Contexte 1
    #     jeu_cle = get_jeu_cle_statique(testDataframes, [2240438311], self.get_context_by_num(sparkSession, 1))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0012_restr_pers_stricte_inclusive1.csv")
    #
    #     restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 1)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)
    #
    # @pytest.mark.xfail
    # def test_restr_person_strict_inclusive_contrat_OK_2(self, sparkSession, testDataframes):
    #     # Demande - Contexte 2
    #     jeu_cle = get_jeu_cle_statique(testDataframes, [2665236111], self.get_context_by_num(sparkSession, 2))
    #
    #     # Expected
    #     df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
    #                                                                                  SPEC_LOT1_PATH_EXPECTED + "0012_restr_pers_stricte_inclusive2.csv")
    #
    #     restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 2)))
    #
    #     assertDataFrameEqual(restitution, df_expected_restitution)

    # Avec des contrats portant des restrictions exclusives sur les personnes dont le type est STRICTE
    def test_restr_person_strict_exclusive_4(self, sparkSession, testDataframes):
        # Demande - Contexte 4
        jeu_cle = get_jeu_cle_statique(testDataframes, [7709493611], self.get_context_by_num(sparkSession, 4))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_person_strict_exclusive_4.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 4)))

        assertDataFrameEqual(restitution, df_expected_restitution)

    @pytest.mark.xfail
    def test_restr_person_strict_exclusive_1(self, sparkSession, testDataframes):
        # Demande - Contexte 1
        jeu_cle = get_jeu_cle_statique(testDataframes,
                                       [6870212011, 7814483211, 7810206511],
                                       self.get_context_by_num(sparkSession, 1))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_person_strict_exclusive_1.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 1)))

        assertDataFrameEqual(restitution, df_expected_restitution)

    # Avec des contrats portant des restrictions exclusives sur les personnes dont le type est NON STRICTE
    @pytest.mark.xfail
    def test_restr_person_non_strict_exclusive_5(self, sparkSession, testDataframes):
        # Demande - Contexte 5
        jeu_cle = get_jeu_cle_statique(testDataframes, [5225283111], self.get_context_by_num(sparkSession, 5))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_person_non_strict_exclusive_5.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 5)))

        assertDataFrameEqual(restitution, df_expected_restitution)

    # Avec des contrats portant des restrictions exclusives sur les personnes dont le type est NON STRICTE
    @pytest.mark.xfail
    def test_restr_person_non_strict_exclusive_1(self, sparkSession, testDataframes):
        # Demande - Contexte 1
        jeu_cle = get_jeu_cle_statique(testDataframes,
                                       [3609566711, 4254593811],
                                       self.get_context_by_num(sparkSession, 1))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_person_non_strict_exclusive_1.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 1)))

        assertDataFrameEqual(restitution, df_expected_restitution)

    # Avec des contrats portant des restrictions sur la famille d'utilisation
    def test_restr_on_family(self, sparkSession, testDataframes):
        # Demande - Contexte 2
        jeu_cle = get_jeu_cle_statique(testDataframes, [3741975311], self.get_context_by_num(sparkSession, 2))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_on_family.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 2)))

        assertDataFrameEqual(restitution, df_expected_restitution)

    #  Avec des contrats portant des restrictions sur le genre d'oeuvre
    # @pytest.mark.xfail
    def test_restr_on_genre(self, sparkSession, testDataframes):
        # Demande - Contexte 5
        jeu_cle = get_jeu_cle_statique(testDataframes, [6940194012], self.get_context_by_num(sparkSession, 5))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "restr_on_genre.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 5)))
        restitution.show()
        assertDataFrameEqual(restitution, df_expected_restitution)

    # Sur le pays d’utilisation
    @pytest.mark.xfail
    def test_pays_utilisation(self, sparkSession, testDataframes):
        # Demande - Contexte 9
        jeu_cle = get_jeu_cle_statique(testDataframes, [4024962211], self.get_context_by_num(sparkSession, 9))

        # Expected
        df_expected_restitution = clean_test_data_load.get_expected_df_from_csv_file(sparkSession,
                                                                                     SPEC_LOT1_PATH_EXPECTED + "pays_utilisation_contexte_9.csv")

        restitution = (get_restitution(testDataframes, jeu_cle, self.get_context_by_num(sparkSession, 9)))

        assertDataFrameEqual(restitution, df_expected_restitution)



