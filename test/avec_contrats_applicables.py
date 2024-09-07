# avec_contrats_applicables.py
import pytest
from pyspark.testing.utils import assertDataFrameEqual
from get_jeu_cle_statique import get_jeu_cle_statique
from get_restitution import get_restitution
from run_test_lot1 import RunTestSpecLot1


def expected_path(path: str) -> str:
    return f"test/test_spec_lot1_expected/avec_contrats_applicables/{path}"


@pytest.mark.usefixtures("setup_class")
class TestAvecContratsApplicables(RunTestSpecLot1):
    def run_test(self, ide12_list, context_num, expected_file):
        jeu_cle = get_jeu_cle_statique(self.test_dataframes, ide12_list, self.get_context_by_num(context_num))

        df_expected_restitution = self.get_expected_dataframe(expected_path(expected_file))
        print("test: expected")
        df_expected_restitution.show()
        restitution = get_restitution(self.chaine_contrats, jeu_cle, self.get_context_by_num(context_num))

        assertDataFrameEqual(restitution, df_expected_restitution)

    def test_0001_one_lettrage(self):
        print("Running test_0001_one_lettrage...")
        self.run_test([5363801411], 1, "0001_one_lettrage.csv")

    def test_0001_no_lettrage(self):
        print("Running test_0001_no_lettrage...")
        self.run_test([6785877311], 5, "0001_no_lettrage.csv")

    def test_0002_many_lettrages(self):
        print("Running test_0002_many_lettrages...")
        self.run_test([3693837711], 6, "0002_many_lettrages.csv")

    def test_0004_all_ad_gestionnaires(self):
        self.run_test([2240438311], 1, "0004_all_ad_gestionnaires.csv")

    def test_0005_no_ad_gestionnaire_on_branch(self):
        self.run_test([4794636111], 1, "0005_no_ad_gestionnaire_on_branch.csv")
