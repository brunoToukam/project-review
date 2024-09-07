import logging
import time
from datetime import datetime
from functools import wraps

from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, size

from data_loader.s3_data_load import get_all_df, get_contrat, write_df
from utils.contrat_builder import build_contrat

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# This main is aim for local run only
if __name__ == "__main__":
    spark = SparkSession.builder.appName("main").getOrCreate()

    data_path = "data/setup_usecases/"
    data = get_all_df(spark, data_path, source="local")

    output = "data/setup_usecases/spec/computed/contrats/"

    contrat_base = True if "contrat_hierarchy_base" in data else False
    max_depth = 4
    date_consultation = datetime.now().strftime("%Y-%m-%d")

    result_df = build_contrat(data, max_depth, remote=False)

    write_df(result_df, output + f"contrat_profondeur_{max_depth}_{date_consultation}", "contrats")
    logger.info("Process completed successfully.")
    use_case = "spec"
    contrat_path = f"data/setup_usecases/{use_case}/computed/contrats/"
    get_contrat(spark, contrat_path, max_depth).show(200)

