from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

SALT_NUMBER = 20


def __rename_cols(df: DataFrame, suffix: str) -> DataFrame:
    for column in df.columns:
        if column not in ["idctr", "iddetcedant"]:
            df = df.withColumnRenamed(column, f"{column}_{suffix}")
    return df


def salt_dataframe(df: DataFrame, col_name: str, salt_number: int) -> DataFrame:
    return df.withColumn(
        f"{col_name}_salted",
        F.concat(F.col(col_name), F.lit("_"), (F.rand() * salt_number).cast(IntegerType())))


def duplicate_salt(df: DataFrame, col_name: str, salt_number: int) -> DataFrame:
    salts = [F.lit(i) for i in range(salt_number)]
    df = df.withColumn("salt", F.explode(F.array(*salts)))
    return df.withColumn(f"{col_name}_salted", F.concat(F.col(col_name), F.lit('_'), F.col("salt"))).drop("salt")


def construct_df_contrat(df_contrat: DataFrame, date_consulatation: str = None) -> DataFrame:
    date_consulatation_col = F.current_date() if date_consulatation is None else F.to_date(F.lit(date_consulatation))

    df_contrat = df_contrat.filter(
        F.col("idctr").isNotNull() &
        # La date de début de validité de contrat doit être inférieure à la date de fin
        (F.when(F.col("datfinctr").isNotNull(), F.col("datdbtctr") < F.col("datfinctr")).otherwise(True)) &
        # La date de début de révision de contrat doit être inférieure à la date de fin
        (F.when(F.col("datfinsys").isNotNull(), F.col("datdbtsys") < F.col("datfinsys")).otherwise(True)) &
        # Filtrer sur la date de consultation
        (F.when(F.col("datfinsys").isNotNull(), date_consulatation_col < F.col("datfinsys")).otherwise(True)) &
        (F.col("datdbtsys") < date_consulatation_col) &  # compare with the consultation date or current date

        (F.col("second") == 0) &  # 1 veut dire que le contrat est secondaire, donc on ne le prend pas
        (F.col("cdestatutctr") != "000") &  # Exclure les contrats non documentés ("000")
        (F.col("cdestatutctr") != "003")  # Exclure les contrats annulés ("003")
    )

    # datfincollect est une surcharge de date de datfinctr
    df_contrat = df_contrat.withColumn(
        "datfinctr",
        F.when(F.col("datfinctr").isNull(), F.col("datfinctr"))
        .when(F.col("expireffetimmediat") == 0,
              F.when(F.col("datfincollect").isNotNull(), F.col("datfincollect"))
              .otherwise(F.col("datfinctr")))
        .otherwise(F.col("datfinctr"))
    )

    drop_cols = ["datfincollect", "expireffetimmediat", "numinf", "datdbtsys", "datfinsys", "cdestatutctr", "second"]
    df_contrat = df_contrat.drop(*drop_cols)

    return df_contrat.alias("df_contrat")


def construct_df_cedant(dfs: dict[str, DataFrame]) -> DataFrame:
    # CEDANT: detcedant = 32 MB, detcedantcatal = 13 MB, pcttypdrt = 61 MB
    df_pcttypdrt = dfs.get("pcttypdrt").drop("idclauspartcl")
    # Add numcatal to cedant
    df_detcedant = (
        dfs.get("detcedant")
        .join(dfs.get("detcedantcatal"), on="iddetcedant").drop("iddetcedant")
    )

    # Add % cédé editeur to cedant
    df_detcedant = (
        df_detcedant.alias('df')
        .join(df_pcttypdrt.alias("df1"), on="idpcttypdrt", how="inner")
        .select(
            # df_detcedant["*"],
            F.col("df.idpcttypdrt"),
            F.col("df.idctr"),
            F.col("df.idpcttypdrtcre"),
            F.col("df.numpers"),
            F.col("df.numcatal"),
            F.coalesce(F.col("df1.pct"), F.lit(0)).alias("pct_editeur"),
            F.col("df1.cdetypdrtsacem").alias("cdetypdrtsacem")
        )
    )

    # Let's add % cédé createur to cedant
    df_detcedant = (
        df_detcedant.alias("df")
        .join(
            df_pcttypdrt.alias("df2"),
            (F.col("df.idpcttypdrtcre") == F.col("df2.idpcttypdrt")) &
            (F.col("df.cdetypdrtsacem") == F.col("df2.cdetypdrtsacem")),
            how='left'
        ).groupBy(
            F.col("df.idctr"),
            F.col("df.numpers"),
            F.col("df.numcatal"),
            F.col("df.pct_editeur"),
            F.coalesce(F.col("df2.pct"), F.lit(0)).alias("pct_createur")
        ).agg(
            F.collect_list(F.col("df.cdetypdrtsacem")).alias("cdetypdrtsacem")
        )

        # select(df_detcedant["*"], F.coalesce(F.col("df2.pct"), F.lit(0)).alias("pct_createur"))
    ).drop_duplicates()

    df_partpt = salt_dataframe(__construct_df_partpt(dfs.get("partpt")), "numcatal", SALT_NUMBER)

    # Duplicate cedant on salt
    df_detcedant = duplicate_salt(df_detcedant, "numcatal", SALT_NUMBER)
    # join
    df_detcedant = df_detcedant.join(df_partpt, on=["numcatal_salted", "numpers"]).select(
        df_detcedant["*"],
        # F.col("let").alias("lettrage"),
        F.col("cdecategadsacem")
    )

    df_detcedant = df_detcedant.drop("numcatal_salted").drop_duplicates()

    df_detcedant = __rename_cols(df=df_detcedant, suffix="cedant")

    df_detcedant = df_detcedant.groupBy(
        "idctr"
    ).agg(
        F.collect_list("numcatal_cedant").alias("numcatal_cedant"),
        F.collect_list("cdecategadsacem_cedant").alias("cdecategadsacem_cedant"),
        F.collect_list("pct_editeur_cedant").alias("pct_editeur_cedant"),
        F.collect_list("pct_createur_cedant").alias("pct_createur_cedant"),
        F.collect_list("cdetypdrtsacem_cedant").alias("cdetypdrtsacem_cedant")
    )

    return df_detcedant


def construct_df_detcess(df_detcess: DataFrame) -> DataFrame:
    # cessionaire size = 47 MB
    df_detcess = df_detcess.drop("iddetcessnr", "datdbtvld", "datfinvld", "idpcttypdrt").drop_duplicates()
    # df_detcess = __duplicate_salt(df_detcess, "numcatal", SALT_NUMBER)

    df_detcess = __rename_cols(
        df=df_detcess,
        suffix="cess"
    ).alias("df_detcess")

    df_detcess = df_detcess.groupBy(
        "idctr"
    ).agg(
        F.collect_list("numcatal_cess").alias("numcatal_cess"),
        F.collect_list("cdecategadsacem_cess").alias("cdecategadsacem_cess"),
        F.collect_list("gestnr_cess").alias("gestnr")
    )

    return df_detcess


def construct_df_restr(dfs: dict[str, DataFrame]) -> DataFrame:
    # Restrictions size: restrct: 220 MB, restrgenre: 15 MB, restroeuv: 151 MB, restrpers: 1 MB
    restr_pers = dfs.get("restrpers").drop("numpers")
    df = (dfs.get("restrctr")
    .join(restr_pers, on="idrestctr", how="left")
    .join(dfs.get("restrgenre"), on="idrestctr", how="left")
    .join(dfs.get("restroeuv"), on="idrestctr", how="left")
    ).drop("idrestctr").drop_duplicates()

    df = df.groupBy("idctr").agg(
        F.collect_list(F.when(F.col("additif") == 1, F.col("cdegreoeuv"))).alias("restr_genre_inclusive"),
        F.collect_list(F.when(F.col("additif") == 0, F.col("cdegreoeuv"))).alias("restr_genre_exclusive"),
        F.collect_list(F.when((F.col("additif") == 1) & (F.col("cdeexignc") == "STRICTE"), F.col("numcatal"))).alias("restr_numcatal_inclusive"),
        F.collect_list(F.when((F.col("additif") == 0) & (F.col("cdeexignc") == "STRICTE"), F.col("numcatal"))).alias("restr_numcatal_exclusive"),
        F.collect_list(F.when(F.col("additif") == 1, F.col("ide12"))).alias("restr_oeuvre_inclusive"),
        F.collect_list(F.when(F.col("additif") == 0, F.col("ide12"))).alias("restr_oeuvre_exclusive")
    )

    return df


def __construct_df_partpt(df_partpt: DataFrame) -> DataFrame:
    selected_cols = ["cdecategadsacem", "numcatal", "numpers"]  # "let",
    return df_partpt.select(selected_cols).drop_duplicates()
