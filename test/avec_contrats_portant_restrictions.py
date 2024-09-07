# avec_contrats_portant_restrictions.py
import pytest
from pyspark.testing.utils import assertDataFrameEqual
from get_jeu_cle_statique import get_jeu_cle_statique
from get_restitution import get_restitution
from run_test_lot1 import RunTestSpecLot1


def expected_path(path: str) -> str:
    return f"test/test_spec_lot1_expected/avec_contrats_portant_restrictions/{path}"


@pytest.mark.usefixtures("setup_class")
class TestAvecContratsPortantRestrictions(RunTestSpecLot1):
    def run_test(self, ide12_list, context_num, expected_file):
        jeu_cle = get_jeu_cle_statique(self.test_dataframes, ide12_list, self.get_context_by_num(context_num))
        df_expected_restitution = self.get_expected_dataframe(expected_path(expected_file))
        restitution = get_restitution(self.test_dataframes, jeu_cle, self.get_context_by_num(context_num))
        assertDataFrameEqual(restitution, df_expected_restitution)

    def test_0010_restr_oeuv_inclusive(self):
        print("Running test_0010_restr_oeuv_inclusive...")
        self.run_test([7504304511], 1, "0010_restr_oeuv_inclusive.csv")

    def test_0011_restr_oeuv_exclusive(self):
        print("Running test_0011_restr_oeuv_exclusive...")
        self.run_test([4373962311, 4372235711], 2, "0011_restr_oeuv_exclusive.csv")

    def test_0012_restr_pers_stricte_inclusive1(self):
        print("Running test_0012_restr_pers_stricte_inclusive1...")
        self.run_test([2240438311], 1, "0012_restr_pers_stricte_inclusive1.csv")

    def test_0012_restr_pers_stricte_inclusive2(self):
        print("Running test_0012_restr_pers_stricte_inclusive2...")
        self.run_test([2665236111], 2, "0012_restr_pers_stricte_inclusive2.csv")
