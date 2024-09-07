import logging
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from setuptools.command.alias import alias

from data_loader.s3_data_load import write_df
from utils import construct_dataframes as C
from utils.construct_dataframes import duplicate_salt, salt_dataframe
from utils.contrat_utils import collect_distinct, collect_intersect, compute_percentage, intersect_restr

logger = logging.getLogger(__name__)


def contrat_base(dfs: dict[str, DataFrame], remote: bool) -> DataFrame:
    df_contrat = C.construct_df_contrat(dfs.get("contrat"))
    df_detcedant = C.construct_df_cedant(dfs)
    df_detcess = C.construct_df_detcess(dfs.get("detcess"))
    df_restr = C.construct_df_restr(dfs)
    ter_cmplx = dfs.get("territories_by_id_tercmplx")

    df = (
        df_contrat
        .join(df_detcedant, on="idctr", how="inner")
        .join(df_detcess, on="idctr", how="inner")
        .join(df_restr, on="idctr", how="left")
        .join(ter_cmplx, on="idtercmplx", how="inner")
    ).select(
        F.array(F.col("numctr")).alias("numctr"),
        F.col("numcatal_cedant"),
        F.col("numcatal_cess"),
        F.col("cdecategadsacem_cedant"),
        F.col("cdecategadsacem_cess"),
        F.col("cdetypctr"),
        F.col("pct_editeur_cedant").alias("pct_editeur"),
        F.col("pct_createur_cedant").alias("pct_createur"),
        F.col("cdetypdrtsacem_cedant").alias("cdetypdrtsacem"),
        F.col("datdbtctr"),
        F.col("datfinctr"),
        F.col("indcsbl"),
        F.col("biem"),
        F.col("idtercess"),
        F.col("fledit"),
        F.coalesce(F.col("restr_genre_inclusive"), F.array()).alias("restr_genre_inclusive"),
        F.coalesce(F.col("restr_genre_exclusive"), F.array()).alias("restr_genre_exclusive"),
        F.coalesce(F.col("restr_numcatal_inclusive"), F.array()).alias("restr_numcatal_inclusive"),
        F.coalesce(F.col("restr_numcatal_exclusive"), F.array()).alias("restr_numcatal_exclusive"),
        F.coalesce(F.when(
            (F.col("cdetypctr") != "OPTION") & (F.col("optaffl") == 0) &
            (F.col("cdecategadsacem_cedant")[0] != "SE"), F.col("restr_oeuvre_inclusive")), F.array()).alias("restr_oeuvre_inclusive"),
        F.coalesce(F.when(
            (F.col("cdetypctr") != "OPTION") & (F.col("optaffl") == 0) &
            (F.col("cdecategadsacem_cedant")[0] != "SE"), F.col("restr_oeuvre_exclusive")), F.array()).alias("restr_oeuvre_exclusive"),
        F.col("territoires")
    )

    logger.info(f"******************* nombre de contrat: {df_contrat.count()}")
    logger.info(f"******************* nombre de df_detcedant: {df_detcedant.count()}")
    logger.info(f"******************* nombre de df_detcess: {df_detcess.count()}")
    logger.info(f"******************* nombre de df_restr: {df_restr.count()}")
    logger.info(f"******************* nombre de ter_cmplx: {ter_cmplx.count()}")

    logger.info(f"*************************nombre de lignes de df: {df.count()}")

    # if remote:
    #     write_df(df,
    #              f"s3a://sacem-perfo-emr-poc-dev/output/contrats_base/contrat_base_{datetime.now().strftime('%Y-%m-%d')}",
    #              "contrat_base")

    logger.info("Contrat base assembled successfully!")

    return df


def higher_levels_hierarchy(df: DataFrame, depth: int) -> DataFrame:
    """Join and aggregate DataFrame based on the specified depth."""
    data = df.repartition(20)
    # L'indice de cessibilité doit être à 1 pour qu'il puisse céder ses parts
    df_cedant = data.where(F.col("indcsbl") == 1).alias("df_cedant")
    df_cess = data.alias("df_cess")
    # salt_number = 10

    if depth == 0:
        joined_df = df_cedant.join(
            df_cess,
            F.expr("size(array_intersect(df_cedant.numcatal_cess, df_cess.numcatal_cedant)) > 0")
        )
    else:
        df_cedant = df_cedant.withColumn("numctr_cedant",
                                         F.expr(f"df_cedant.numctr[size(df_cedant.numctr) - {depth}]")).alias("df_cedant")
        df_cess = df_cess.withColumn("numctr_cess", F.expr("df_cess.numctr[0]")).alias("df_cess")

        joined_df = df_cedant.join(
            df_cess, F.col("numctr_cedant") == F.col("numctr_cess"), "inner"
        )

    selected_df = joined_df.select(
        F.array_distinct(F.array_union("df_cedant.numctr", "df_cess.numctr")).alias("numctr"),
        F.col('df_cedant.numcatal_cedant'),
        F.col('df_cess.numcatal_cess'),
        F.col('df_cedant.cdecategadsacem_cedant'),
        F.col('df_cess.cdecategadsacem_cess'),
        F.col('df_cedant.cdetypctr'),
        F.when(
            F.col("df_cess.cdecategadsacem_cedant")[0] == "E",
            F.transform(
                F.col("df_cedant.pct_editeur"),
                lambda x, i: (x * F.col("df_cess.pct_editeur")[i] / 100).cast("int")
            )
        ).otherwise(F.col("df_cedant.pct_editeur")).alias("pct_editeur"),
        F.col("df_cedant.pct_createur"),
        F.col('df_cedant.cdetypdrtsacem'),
        F.when(
            F.col("df_cedant.datdbtctr") < F.col("df_cess.datdbtctr"), F.col("df_cess.datdbtctr")
        ).otherwise(F.col("df_cedant.datdbtctr")).alias('datdbtctr'),
        F.when(
            F.col("df_cedant.datfinctr") > F.col("df_cess.datfinctr"), F.col("df_cess.datfinctr")
        ).otherwise(F.col("df_cedant.datfinctr")).alias('datfinctr'),
        F.col("df_cess.indcsbl"),
        F.col('df_cedant.biem'),
        F.col('df_cedant.idtercess'),
        F.col("df_cedant.fledit"),
        # F.array_distinct(F.array_union("df_cedant.restr_genre_inclusive", "df_cess.restr_genre_inclusive")).alias("restr_genre_inclusive"),
        intersect_restr("df_cedant.restr_genre_inclusive", "df_cess.restr_genre_inclusive").alias("restr_genre_inclusive"),
        F.array_distinct(F.array_union("df_cedant.restr_genre_exclusive", "df_cess.restr_genre_exclusive")).alias("restr_genre_exclusive"),
        # F.array_distinct(F.array_intersect("df_cedant.restr_numcatal_inclusive", "df_cess.restr_numcatal_inclusive")).alias("restr_numcatal_inclusive"),
        intersect_restr("df_cedant.restr_numcatal_inclusive", "df_cess.restr_numcatal_inclusive").alias("restr_numcatal_inclusive"),
        F.array_distinct(F.array_union("df_cedant.restr_numcatal_exclusive", "df_cess.restr_numcatal_exclusive")).alias(
            "restr_numcatal_exclusive"),
        # F.array_distinct(F.array_union("df_cedant.restr_oeuvre_inclusive", "df_cess.restr_oeuvre_inclusive")).alias("restr_oeuvre_inclusive"),
        intersect_restr("df_cedant.restr_oeuvre_inclusive", "df_cess.restr_oeuvre_inclusive").alias("restr_oeuvre_inclusive"),
        F.array_distinct(F.array_union("df_cedant.restr_oeuvre_exclusive", "df_cess.restr_oeuvre_exclusive")).alias("restr_oeuvre_exclusive"),
        F.array_distinct(F.array_intersect("df_cedant.territoires", "df_cess.territoires")).alias("territoires")
    ).dropDuplicates()

    filtered_df = selected_df.filter(
        ~F.array_contains(F.col("restr_genre_inclusive"), "empty") &
        ~F.array_contains(F.col("restr_numcatal_inclusive"), -1) &
        ~F.array_contains(F.col("restr_oeuvre_inclusive"), -1) &
        (F.size(F.col("territoires")) > 0)
    )

    return filtered_df


def build_contrat(dfs: dict[str, DataFrame], max_depth: int, remote=True) -> DataFrame:
    """
    We want to make the column 'numctr' the unique identifier of each line.
    So we aggregate on others columns and selected only desired columns

    Parameters
    ----------
    dfs: dict[str, DataFrame]
        There are dataframes necessary to process the function process_contrat_hierarchy
    contrat_base_exists: bool
        True if contrat_hierarchy_base is already compute and save in s3, so we just read it instead of load all data
    max_depth: int
        the maximal depth we want for our hierarchy
    output: str
        The path ta save contracts: current = "s3a://sacem-perfo-emr-poc-dev/output/contrats/"
    remote: bool
        Whether the run is locally or remotely. By default, True; so we need to set it to False before running locally

    returns
    -------
    DataFrame
    """
    logger.info(f"Generating contracts matrix with max depth {max_depth}...")

    # if contrat_base_exists:
    #     logger.info("contrat_hierarchy_base already exists, let's load it.")
    #     first_level_df = dfs.get("contrat_hierarchy_base")
    # else:
    logger.info("Building contrat_base...")
    first_level_df = contrat_base(dfs, remote)

    # Process hierarchical aggregation of the DataFrame up to the specified maximum depth
    final_df = first_level_df

    for depth in range(max_depth):
        next_level_df = higher_levels_hierarchy(first_level_df, depth)

        logger.info(f"depth {depth + 1} complete")
        first_level_df = next_level_df
        final_df = final_df.unionByName(next_level_df)


    final_df = final_df.select(
        F.array_distinct("numctr").alias("numctr"),
        "numcatal_cedant",
        "numcatal_cess",
        "cdecategadsacem_cedant",
        "cdecategadsacem_cess",
        "cdetypctr",
        "pct_editeur",
        "pct_createur",
        "cdetypdrtsacem",
        "datdbtctr",
        "datfinctr",
        "biem",
        "idtercess",
        "fledit",
        F.array_distinct("restr_genre_inclusive").alias("restr_genre_inclusive"),
        F.array_distinct("restr_genre_exclusive").alias("restr_genre_exclusive"),
        F.array_distinct("restr_numcatal_inclusive").cast("array<long>").alias("restr_numcatal_inclusive"),
        F.array_distinct("restr_numcatal_exclusive").cast("array<long>").alias("restr_numcatal_exclusive"),
        F.array_distinct("restr_oeuvre_inclusive").cast("array<long>").alias("restr_oeuvre_inclusive"),
        F.array_distinct("restr_oeuvre_exclusive").cast("array<long>").alias("restr_oeuvre_exclusive"),
        F.array_distinct("territoires").alias("territoires")
    ).drop_duplicates()

    # output_path = f"{output}contrat_profondeur_{depth + 1}_{date_consultation}/"
    # write_df(final_df, output_path, "contrats")

    logger.info("Finished loop")

    return final_df


if __name__ == '__main__':
    exit()
