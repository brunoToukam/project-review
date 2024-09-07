import logging
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from data_loader.s3_data_load import write_df
from utils import construct_dataframes as C
from utils.construct_dataframes import duplicate_salt, salt_dataframe

logger = logging.getLogger(__name__)


def join_dataframes(dfs: dict[str, DataFrame]) -> DataFrame:
    df_contrat = C.construct_df_contrat(dfs.get("contrat"))
    df_detcedant = C.construct_df_cedant(dfs)
    df_detcess = C.construct_df_detcess(dfs.get("detcess"))
    df_restr = C.construct_df_restr(dfs)

    # contrat + cedant + cess + restr
    df = (df_contrat
          .join(df_detcedant, on="idctr", how="inner")
          .join(df_detcess, on="idctr", how="inner")
          .join(df_restr, on="idctr", how="left")
          )

    logger.info("Adding territories_by_id_tercmplx...")
    df = df.join(
        dfs.get("territories_by_id_tercmplx"),
        on="idtercmplx",
        how="inner"
    )

    return df.alias("df")


def contrat_hierarchy_base(dataframes: dict[str, DataFrame], remote: bool) -> DataFrame:
    df = join_dataframes(dataframes)

    df = df.groupBy(
        F.array(F.col("numctr")).alias("numctr"),
        F.array(F.col("numinf")).alias("numinf"),
        F.col("cdetypctr"),
        F.col("indcsbl"),
        F.col("biem"),
        F.col("idtercess"),
        F.col("fledit"),
        F.col("datdbtctr"),
        F.col("datfinctr"),
        F.col("numpers_cedant"),
        F.col("cdetypdrtsacem_cedant").alias("cdetypdrtsacem"),
        F.col("pct_editeur_cedant").alias("pct_editeur"),
        F.col("pct_createur_cedant").alias("pct_createur"),
        F.col("lettrage_cedant"),
        F.col("numcatal_cedant"),
        F.col("cdecategadsacem_cedant"),
        F.col("cdecategadsacem_cess"),
        F.col("numcatal_cess"),
        F.col("numpers_cess"),
        F.col("territoires")
    ).agg(
        F.collect_list(F.when(F.col("additif_restr") == 1, F.col("cdegreoeuv_restr"))).alias("restr_genre_include"),
        F.collect_list(F.when(F.col("additif_restr") == 0, F.col("cdegreoeuv_restr"))).alias("restr_genre_exclude"),
        F.collect_list(F.when(F.col("additif_restr") == 1, F.col("numcatal_restr"))).alias("restr_numcatal_include"),
        F.collect_list(F.when(F.col("additif_restr") == 0, F.col("numcatal_restr"))).alias("restr_numcatal_exclude"),
        F.collect_list(
            F.when(
                (F.col("cdetypctr") != "OPTION") &
                (F.col("optaffl") == 0) &
                (F.col("cdecategadsacem_cedant") != "SE") &
                (F.col("additif_restr") == 1),
                F.col("ide12_restr")
            )
        ).alias("restr_oeuvre_include"),
        F.collect_list(
            F.when(
                (F.col("cdetypctr") != "OPTION") &
                (F.col("optaffl") == 0) &
                (F.col("cdecategadsacem_cedant") != "SE") &
                (F.col("additif_restr") == 0),
                F.col("ide12_restr")
            )
        ).alias("restr_oeuvre_exclude"),
        F.collect_list(F.col("cdeexignc_restr")).alias('restr_pers_exigence')
    ).drop_duplicates()

    # df = df.repartition("numcatal_salted_cedant")
    logger.info("Base hierarchy complete")

    if remote:
        write_df(df,
                 f"s3a://sacem-perfo-emr-poc-dev/output/contrats_base/contrat_base_{datetime.now().strftime('%Y-%m-%d')}",
                 "contrat_base")

    return df


def higher_levels_hierarchy(df: DataFrame, depth: int) -> DataFrame:
    """Join and aggregate DataFrame based on the specified depth."""
    # L'indice de cessibilité doit être à 1 pour qu'il puisse céder ses parts
    df_cedant = df.where(F.col("indcsbl") == 1).alias("df_cedant")
    df_cess = df.alias("df_cess")

    if depth == 0:
        salt_number = 10
        df_cedant = duplicate_salt(df_cedant, "numcatal_cess", salt_number).alias('df_cedant')
        df_cess = salt_dataframe(df_cess, "numcatal_cedant", salt_number).alias("df_cess")

        joined_df = df_cedant.join(
            df_cess,
            F.col('df_cedant.numcatal_cess_salted') == F.col('df_cess.numcatal_cedant_salted')
        )
    else:
        df_cedant = df_cedant.withColumn("numctr_cedant", F.expr("df_cedant.numctr[size(df_cedant.numctr) - 1]"))
        df_cess = df_cess.withColumn("numctr_cess", F.expr("df_cess.numctr[0]")).alias("df_cess")

        if depth == 1:
            df_cedant = df_cedant.repartition("numctr_cedant")
            df_cess = df_cess.repartition("numctr_cess")

        joined_df = df_cedant.join(
            df_cess,
            F.col("numctr_cedant") == F.col("numctr_cess")
        )

    selected_df = joined_df.select(
        F.col('df_cedant.numcatal_cedant'),
        F.col('df_cess.numcatal_cess'),
        F.col('df_cedant.lettrage_cedant'),
        F.col("df_cess.indcsbl"),
        F.col('df_cedant.cdecategadsacem_cedant'),
        F.col('df_cess.cdecategadsacem_cess'),
        F.col('df_cedant.cdetypctr'),
        F.col("df_cedant.numpers_cedant"),
        F.col("df_cess.numpers_cess"),
        F.col('df_cedant.cdetypdrtsacem'),
        F.col('df_cedant.biem'),
        F.col('df_cedant.idtercess'),
        F.col("df_cedant.fledit"),
        F.when(
            F.col("df_cedant.datdbtctr") < F.col("df_cess.datdbtctr"), F.col("df_cess.datdbtctr")
        ).otherwise(F.col("df_cedant.datdbtctr")).alias('datdbtctr'),
        F.when(
            F.col("df_cedant.datfinctr") > F.col("df_cess.datfinctr"), F.col("df_cess.datfinctr")
        ).otherwise(F.col("df_cedant.datfinctr")).alias('datfinctr'),
        F.array_union("df_cedant.numctr", "df_cess.numctr").alias("numctr"),
        F.array_union("df_cedant.numinf", "df_cess.numinf").alias("numinf"),
        F.array_intersect("df_cedant.territoires", "df_cess.territoires").alias("territoires"),
        F.array_union("df_cedant.restr_genre_include", "df_cess.restr_genre_include").alias("restr_genre_include"),
        F.array_union("df_cedant.restr_genre_exclude", "df_cess.restr_genre_exclude").alias("restr_genre_exclude"),
        F.array_intersect("df_cedant.restr_numcatal_include", "df_cess.restr_numcatal_include").alias(
            "restr_numcatal_include"),
        F.array_union("df_cedant.restr_numcatal_exclude", "df_cess.restr_numcatal_exclude").alias(
            "restr_numcatal_exclude"),
        F.array_union("df_cedant.restr_pers_exigence", "df_cess.restr_pers_exigence").alias("restr_pers_exigence"),
        F.array_union("df_cedant.restr_oeuvre_include", "df_cess.restr_oeuvre_include").alias("restr_oeuvre_include"),
        F.array_union("df_cedant.restr_oeuvre_exclude", "df_cess.restr_oeuvre_exclude").alias("restr_oeuvre_exclude"),

        F.when(F.col("df_cess.cdecategadsacem_cedant") == "E",
               (F.col("df_cedant.pct_editeur") * F.col(f"df_cess.pct_editeur") / 100).alias("pct_editeur")
               ).otherwise(F.col("df_cedant.pct_editeur")).alias("pct_editeur"),
        F.col("df_cedant.pct_createur")
    ).drop_duplicates().alias("df")

    # aggregated_df = (
    #     joined_df.groupBy(
    #         F.col('df_cedant.numcatal_cedant'),
    #         F.col('df_cess.numcatal_cess'),
    #         F.col('df_cedant.lettrage_cedant'),
    #         F.col("df_cess.indcsbl"),
    #         F.col('df_cedant.cdecategadsacem_cedant'),
    #         F.col('df_cess.cdecategadsacem_cess'),
    #         F.col('df_cedant.cdetypctr'),
    #         F.col("df_cedant.numpers_cedant"),
    #         F.col("df_cess.numpers_cess"),
    #         F.col('df_cedant.cdetypdrtsacem'),
    #         F.col('df_cedant.biem'),
    #         F.col('df_cedant.idtercess'),
    #         F.col("df_cedant.fledit"),
    #         F.col("df_cess.cdecategadsacem_cedant").alias("cess_cdecategadsacem_cedant"),
    #         F.when(
    #             F.col("df_cedant.datdbtctr") < F.col("df_cess.datdbtctr"), F.col("df_cess.datdbtctr")
    #         ).otherwise(F.col("df_cedant.datdbtctr")).alias('datdbtctr'),
    #         F.when(
    #             F.col("df_cedant.datfinctr") > F.col("df_cess.datfinctr"), F.col("df_cess.datfinctr")
    #         ).otherwise(F.col("df_cedant.datfinctr")).alias('datfinctr'),
    #         F.col("df_cedant.numcatal_salted_cedant"),
    #         F.col("df_cess.numcatal_salted_cess")
    #     ).agg(
    #         collect_distinct("numctr"),
    #         collect_distinct("numinf"),
    #         collect_intersect("territoires"),
    #         collect_distinct("restr_genre_include"),
    #         collect_distinct("restr_genre_exclude"),
    #         collect_intersect("restr_numcatal_include"),
    #         collect_distinct("restr_numcatal_exclude"),
    #         collect_distinct("restr_pers_exigence"),
    #         collect_distinct("restr_oeuvre_include"),
    #         collect_distinct("restr_oeuvre_exclude"),
    #         compute_percentage("pct_editeur"),
    #         compute_percentage("pct_createur"),
    #     ).drop("cess_cdecategadsacem_cedant").drop_duplicates()
    # )

    return selected_df


def process_contrat_hierarchy(dataframes: dict[str, DataFrame], max_depth: int, contrat_base: bool,
                              remote: bool) -> DataFrame:
    logger.info("Generating contracts matrix with max depth {}...".format(max_depth))

    if contrat_base:
        logger.info("contrat_hierarchy_base already exists, let's load it.")
        first_level_df = dataframes.get("contrat_hierarchy_base")
    else:
        logger.info("Building contrat_hierarchy_base...")
        first_level_df = contrat_hierarchy_base(dataframes, remote)

    # Cache the first level dataframe for reuse
    first_level_df.cache()

    # Process hierarchical aggregation of the DataFrame up to the specified maximum depth
    final_df = first_level_df

    for depth in range(max_depth):
        next_level_df = higher_levels_hierarchy(first_level_df, depth)

        # if not next_level_df.head(1):
        #    break

        logger.info(f"profondeur {depth + 1} complete!")
        first_level_df = next_level_df

        final_df = final_df.unionByName(next_level_df)

    # Unpersist the cached dataframe after processing to free garbage collector
    first_level_df.unpersist()

    return final_df


def build_contrat(dfs: dict[str, DataFrame], max_depth: int, contrat_base: bool = False, remote=True) -> DataFrame:
    """
    We want to make the column 'numctr' the unique identifier of each line.
    So we aggregate on others columns and selected only desired columns

    Parameters
    ----------
    dfs: dict[str, DataFrame]
        There are dataframes necessary to process the function process_contrat_hierarchy
    contrat_base: bool
        True if contrat_hierarchy_base is already compute and save in s3, so we just read it instead of load all data
    max_depth: int
        the maximal depth we want for our hierarchy
    remote: bool
        Whether the run is loccaly or remotely. By default, True; so we need to set it to False before running locally

    returns
    -------
    DataFrame
    """
    df = process_contrat_hierarchy(dfs, max_depth, contrat_base, remote)

    result_df = df.select(
        F.array_distinct("numctr").alias("numctr"),
        F.array_distinct("numinf").cast("array<int>").alias("numinf"),
        "cdetypctr",
        "cdecategadsacem_cedant",
        "numcatal_cedant",
        "numpers_cedant",
        "cdecategadsacem_cess",
        "numcatal_cess",
        "numpers_cess",
        "pct_editeur",
        "pct_createur",
        "datdbtctr",
        "datfinctr",
        "biem",
        "idtercess",
        "fledit",
        F.array_distinct("restr_oeuvre_include").cast("array<long>").alias("restr_oeuvre_include"),
        F.array_distinct("restr_oeuvre_exclude").cast("array<long>").alias("restr_oeuvre_exclude"),
        F.array_distinct("restr_genre_include").alias("restr_genre_include"),
        F.array_distinct("restr_genre_exclude").alias("restr_genre_exclude"),
        F.array_distinct("restr_numcatal_include").cast("array<long>").alias("restr_numcatal_include"),
        F.array_distinct("restr_numcatal_exclude").cast("array<long>").alias("restr_numcatal_exclude"),
        F.array_distinct("restr_pers_exigence").alias("restr_pers_exigence"),
        F.array_distinct("territoires").alias("territoires"),
        F.col("cdetypdrtsacem"),
        F.col("lettrage_cedant")
    ).drop_duplicates()

    return result_df


def save_contrat(df: DataFrame, output_path: str, profondeur: int, date_consultation=None) -> None:
    if not date_consultation:
        date_consultation = datetime.now().strftime("%Y-%m-%d")

    output_path = f"{output_path}contrat_profondeur_{profondeur}_{date_consultation}/"
    write_df(df, output_path, "contrat")


if __name__ == '__main__':
    exit()
