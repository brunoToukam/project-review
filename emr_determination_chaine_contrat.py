from datetime import datetime

from pyspark.sql import SparkSession
from data_loader.s3_data_load import get_all_df, write_df
from utils.contrat_builder import build_contrat
import argparse
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', help="S3 containing parquet files.")
    parser.add_argument('--output_uri', help="S3 path for result.")
    parser.add_argument('--max_depth', help="Max depth for contract chain.")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ContratHierarchy").getOrCreate()

    logger.info("Spark session created successfully.")

    # Load dataframes from S3
    data = get_all_df(spark_session=spark, data_path=args.data_source)

    contrat_base = True if "contrat_hierarchy_base" in data else False
    max_depth = int(args.max_depth)
    date_consultation = datetime.now().strftime("%Y-%m-%d")
    output = f"{args.output_uri}contrats/"

    result_df = build_contrat(data, max_depth)

    # Save the results
    write_df(result_df.coalesce(1), output + f"contrat_profondeur_{max_depth}_{date_consultation}", "contrats")

    logger.info("Process completed successfully.")

# Environ 20 GB = 20480 MB de données
# 1 tâche = 1 core = 1 partition = 128 MB: 20480/128 = 160 partitions/cpu
# Spark recommande 2 à 5 tâches par exécuteur. Prenons 4. 160/4 = 40 exécuteurs (pour core 6 => 32 executeurs)
# Spark recommande: mémoire d'un coeur minimum 4*(taille d'une partition)= 4*128MB = 512 MB / core
# Ayant 4 core * 512 = 2048 MB = 2 GB par exécuteurs (40)
# Soit nous pouvons choisir 20 exécuteurs de 4 GB chacun
# Exécuteurs: 40 => RAM: 4GB = 160 GB
#               => CPU: 4 core = 160 core
# Driver: 1 => RAM: 7GB
#           => CPU: 2 core