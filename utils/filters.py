from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, when, to_date, lit


def filter_contrat(df: DataFrame, date_consulatation: str = None) -> DataFrame:
    date_consulatation_col = current_date() if date_consulatation is None else to_date(lit(date_consulatation))

    df = df.where(
        # La date de début de validité de contrat doit être inférieure à la date de fin
        (when(col("datfinctr").isNotNull(), col("datdbtctr") < col("datfinctr")).otherwise(True)) &
        # La date de début de révision de contrat doit être inférieure à la date de fin
        (when(col("datfinsys").isNotNull(), col("datdbtsys") < col("datfinsys")).otherwise(True)) &
        # Filtrer sur la date de consultation
        (when(col("datfinsys").isNotNull(), date_consulatation_col < col("datfinsys")).otherwise(True)) &
        (col("datdbtsys") < date_consulatation_col) &  # compare with the consultation date or current date

        (col("second") != 1) &  # 1 veut dire que le contrat est secondaire, donc on ne le prend pas
        (col("cdestatutctr") != "000") &  # Exclure les contrats non documentés ("000")
        (col("cdestatutctr") != "003")  # Exclure les contrats annulés ("003")
    )

    # datfincollect est une surcharge de date de datfinctr
    df = df.withColumn(
        "datfinctr",
        when(col("datfinctr").isNull(), col("datfinctr"))
        .when(col("expireffetimmediat") == 0,
              when(col("datfincollect").isNotNull(), col("datfincollect"))
              .otherwise(col("datfinctr")))
        .otherwise(col("datfinctr"))
    )
    df = df.drop("datfincollect", "expireffetimmediat")

    return df.alias("df_contrat")


def filter_oeuv(df_oeuv, date_consultation: str, list_ide12: list[int]) -> DataFrame:
    print("Filtering oeuv...")
    return df_oeuv.filter(
        (col('datdbtsys') < to_date(lit(date_consultation))) &
        ((col('datfinsys') > to_date(lit(date_consultation))) | col('datfinsys').isNull()) &
        (col('ide12').isin(list_ide12))
    )
