import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, array, lit, concat_ws, col

from get_restitution import get_restitution
from data_loader import clean_test_data_load
from get_jeu_cle_statique import get_jeu_cle_statique
from pyspark.testing.utils import assertDataFrameEqual

import pytest

SPEC_LOT1_PATH_EXPECTED = "test/test_spec_lot1_expected/"


class TestSpecLot1:

    # En fonction de la date de
    @pytest.mark.xfail
    def test_date_consultation_7(self):
        raise NotImplemented()

    @pytest.mark.xfail
    def test_date_consultation_8(self):
        raise NotImplemented()
