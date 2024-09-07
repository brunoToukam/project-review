from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def __filter_base_chain_contract_length_and_priority(df: DataFrame) -> DataFrame:
    df = df.withColumn("first_element", F.expr("chaine_contrats[0]"))
    df = df.withColumn("chaine_contrats_length", F.size("chaine_contrats"))

    window_spec = Window.partitionBy("first_element")

    max_length_df = (
        df.withColumn("max_length", F.max("chaine_contrats_length").over(window_spec))
        .withColumn("max_priort", F.max("priort").over(window_spec))
    )

    return max_length_df.filter(
        (F.col("chaine_contrats_length") == F.col("max_length")) &
        (F.col("priort") == F.col("max_priort"))
    ).drop("first_element", "chaine_contrats_length", "max_length", "max_priort", "priort")


def get_restitution(df_contrats, jeu_cle_original, context):
    df = jeu_cle_original.join(
        df_contrats.alias("chaine_contrats"),
        on=(jeu_cle_original.numcatal == df_contrats.numcatal_cedant) &
           (F.array_contains(df_contrats.territoires, context["code_iso4n_pays"])) &
           (df_contrats.cdetypdrtsacem == jeu_cle_original.cdetypdrtsacem),
        how="left_outer"
    )

    d = df.select(
        jeu_cle_original.ide12,
        jeu_cle_original.let.alias("lettrage"),
        jeu_cle_original.numcatal.alias("numcatal_cedant_origine"),

        F.when(F.col("chaine_contrats.cdecategadsacem_cedant").isNotNull(), df_contrats.cdecategadsacem_cedant)
        .otherwise(jeu_cle_original.cdecategadsacem).alias("role_ad_origine"),

        F.when(F.col("chaine_contrats.numcatal_cess").isNotNull(), df_contrats.numcatal_cess)
        .otherwise(jeu_cle_original.numcatal).alias("numcatal_ayant_droit_final"),

        F.when(F.col("chaine_contrats.cdecategadsacem_cess").isNotNull(), df_contrats.cdecategadsacem_cess)
        .otherwise(jeu_cle_original.cdecategadsacem).alias("role_ad_final"),

        F.when(F.size(F.col("chaine_contrats.numctr")).isNotNull(), df_contrats.numctr).otherwise(None).alias(
            "chaine_contrats"),

        "chaine_contrats.pct_editeur",
        "chaine_contrats.pct_createur",
        "cle",
        "priort",
        "gestnr",
        "restr_oeuvre_include",
        "restr_oeuvre_exclude",
        "restr_genre_include",
        "restr_genre_exclude",
        "restr_numcatal_include",
        "restr_numcatal_exclude",
        "restr_pers_exigence"
    )

    d = __filter_base_chain_contract_length_and_priority(d).drop_duplicates()

    window_spec_prorata = Window.partitionBy("ide12", "lettrage")

    d = d.withColumn(
        "sum_cle_gestnr_1",
        F.sum(F.when((F.col("role_ad_origine") == 'E') & (F.col("gestnr") == 1), F.col("cle")).otherwise(0)).over(
            window_spec_prorata)
    ).withColumn(
        "sum_cle_crea",
        F.sum(F.when((F.col("role_ad_origine").isin('A', 'C', 'CA')), F.col("cle")).otherwise(0)).over(
            window_spec_prorata)
    ).withColumn(
        "prorata",
        F.when((F.col("role_ad_origine") == 'E') & (F.col("gestnr") == 1),
               (F.col("cle") / F.col("sum_cle_gestnr_1")) * 100)
        .when((F.col("role_ad_origine").isin('A', 'C', 'CA')), (F.col("cle") / F.col("sum_cle_crea")) * 100)
    )

    pct_editeur_e_gestnr_1 = F.first(
        F.when((F.col("role_ad_origine") == 'E') & (F.col("gestnr") == 1), F.col("pct_editeur")),
        ignorenulls=True).over(window_spec_prorata)
    pct_createur_e_gestnr_1 = F.first(
        F.when((F.col("role_ad_origine") == 'E') & (F.col("gestnr") == 1), F.col("pct_createur")),
        ignorenulls=True).over(window_spec_prorata)

    d = d.withColumn(
        "pct_partition",
        F.coalesce(
            F.when((F.col("role_ad_origine") == 'E') & (F.col("gestnr") == 0), pct_editeur_e_gestnr_1)
            .when((F.col("role_ad_origine").isin('A', 'C', 'CA')), pct_createur_e_gestnr_1 * F.col("prorata") / 100)
            .otherwise(F.col("pct_editeur")),
            F.lit(0))
    )

    d = d.withColumn("pct_cede",
                     F.when(F.sum(F.when(F.col("gestnr") != "0", 1).otherwise(0)).over(window_spec_prorata) > 0,
                            F.col("cle") * F.col("pct_partition") / 100)
                     .otherwise(
                         F.when((F.col("role_ad_origine").isin('A', 'C', 'CA')),
                                F.col("cle") * F.col("pct_createur") / 100)
                         .otherwise(F.col("cle") * F.col("pct_editeur") / 100)
                     ))
    d = d.withColumn("pct_restant", F.col("cle") - F.coalesce(F.col("pct_cede"), F.lit(0)))

    d = d.withColumn("pct_final",
                     F.when((F.col("chaine_contrats").isNotNull()) & (F.col("gestnr") == 0), F.col('pct_cede'))
                     .when((F.col("chaine_contrats").isNotNull()) & (F.col("gestnr") == 1),
                           F.sum(
                               F.when(F.col("gestnr") == 0, F.col("pct_cede"))
                           ).over(window_spec_prorata) + F.col("pct_cede")
                           )
                     .otherwise(F.col("pct_restant")))

    d = d.withColumn(
        "pct_final",
        F.when(F.array_contains(F.col("restr_oeuvre_exclude"), F.col("ide12")), F.col("cle")).otherwise(
            F.col("pct_final"))
    ).withColumn(
        "numcatal_ayant_droit_final",
        F.when(F.array_contains(F.col("restr_oeuvre_exclude"), F.col("ide12")),
               F.col("numcatal_cedant_origine")).otherwise(
            F.col("numcatal_ayant_droit_final"))
    ).withColumn(
        "chaine_contrats",
        F.when(F.array_contains(F.col("restr_oeuvre_exclude"), F.col("ide12")), F.lit(None)).otherwise(
            F.col("chaine_contrats"))
    ).withColumn(
        "role_ad_final",
        F.when(F.array_contains(F.col("restr_oeuvre_exclude"), F.col("ide12")), F.col("role_ad_origine")).otherwise(
            F.col("role_ad_final"))
    )

    window_spec = Window.partitionBy("ide12", "lettrage", "numcatal_cedant_origine")
    d = d.withColumn("cle_sum_by_group", F.sum("cle").over(window_spec))

    new_lines = d.filter((F.col("chaine_contrats").isNotNull()) &
                         (F.col("pct_editeur") != 100) &
                         (F.col("cle_sum_by_group") != 100))
    new_lines = (
        new_lines.withColumn("numcatal_ayant_droit_final", F.col("numcatal_cedant_origine"))
        .withColumn("role_ad_final", F.col("role_ad_origine"))
        .withColumn("chaine_contrats", F.lit(None))
        .withColumn("pct_final", F.col("pct_restant"))
    )

    d = d.union(new_lines)

    d = d.select("ide12", "lettrage", "numcatal_cedant_origine", "role_ad_origine", "numcatal_ayant_droit_final",
                 "role_ad_final", "chaine_contrats",
                 F.round(F.col("pct_final"), 4).alias("cle")).drop_duplicates()

    print("restit df")
    d.show()
    return d
