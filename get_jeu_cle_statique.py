from pyspark.sql import functions as F, DataFrame


def get_jeu_cle_statique(dfs: dict[str, DataFrame], list_oeuvres, context):
    df_oeuv = (
        dfs["oeuv"].select("idoeuv", "ide12", "datdbtsys", "datfinsys")
        .filter(F.col("ide12").isin(list_oeuvres))
        .filter(
            (F.col("datdbtsys") < context["date_consultation"]) &
            ((F.col("datfinsys") > context["date_consultation"]) | (F.col("datfinsys").isNull()))
        )
    ).drop_duplicates()
    df_cess = dfs["cess"].select("idcess", "idoeuv", "idtercmplx", "priort").drop_duplicates()
    df_jeucle = dfs["jeucle"].select("idjeucle", "cdetypdrtsacem", "idcess").filter(
        F.col("cdetypdrtsacem") == context["code_type_droit"]).drop_duplicates()
    df_ayant_droit = dfs["ayant_droit"].select("idayantdrt", "idjeucle", "idpartpt", "cle", "cedant", "gestnr",
                                               "cde_motif_mise_en_attente").drop_duplicates()
    df_partpt = dfs["partpt"].select("idoeuv", "idpartpt", "numpers", "numcatal", "cdecategadsacem", "let").drop_duplicates()
    df_terr_by_id = dfs["territories_by_id_tercmplx"].filter(
        F.array_contains(F.col("territoires"), context["code_iso4n_pays"]))

    return (
        df_oeuv
        .join(df_cess.alias("df_cess"), on="idoeuv")
        .join(df_terr_by_id.alias("df_terr_by_id"), on="idtercmplx")
        .join(df_jeucle, on="idcess")
        .join(df_ayant_droit, on="idjeucle")
        .join(df_partpt.alias("df_partpt"), on=["idoeuv", "idpartpt"])
        .select("idoeuv", "ide12", "cdetypdrtsacem", "numpers", "numcatal", "cle", "cdecategadsacem", "let",
                "cde_motif_mise_en_attente", "idtercmplx", "territoires", "cedant", "gestnr", "priort")
    ).drop_duplicates()

# oeuv: idoeuv, ide12, datdbtsys, datfinsys
# cess: idcess, idoeuv, idtercmplx, priort
# jeucle: idjeucle, cdetypdrtsacem, idcess
# ayant_droit: idayantdrt, idjeucle, idpartpt, cle, cedant, gestnr, cde_motif_mise_en_attente
# partpt: idpartpt, numpers, numcatal, cdecategadsacem, let
# territories_by_id_tercmplx: idtercmplx, territoires


# idoeuv,cdestatutoeuv,paysori,cdegreoeuv,ide12,datdepot,datcre,datdbtsys,datfinsys,toreindex,datmaj,datmajtech,publierevt
# idcess,idtercmplx,priort,idoeuv,datdbtsys,datfinsys
# idjeucle,datdbtsys,datfinsys,datdbtvld,datfinvld,cdetypdrtsacem,idcess
# idayantdrt,idjeucle,idpartpt,cle,numcatalrepst,cedant,gestnr,comacc,cde_motif_mise_en_attente,droit_mise_en_attente
