from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, collect_set, collect_list, udf, last, array_agg, array, create_map, struct
from pyspark.sql.types import IntegerType, LongType, ArrayType


def get_territories_computed(df):
    included_territory = df.select(df.cdepaysiso4npere.alias("id"), df.cdepaysiso4nfils.alias("id_child"))

    # Iteratively apply the union operation
    while True:
        old_count = included_territory.count()

        # Join with the original DataFrame and apply union operation
        new_rows = included_territory.alias("pere").join(included_territory.alias("fils"),
                                                         col("fils.id") == col("pere.id_child")) \
            .select("pere.id", "fils.id_child")
        included_territory = included_territory.union(new_rows).distinct()
        new_count = included_territory.count()

        # If the count does not change, we have reached the end of recursion
        if old_count == new_count:
            break

    # Create a DataFrame for leaves
    leaves = df.alias("leaves").join(df.alias("branch"),
                                     col("branch.cdepaysiso4npere") == col("leaves.cdepaysiso4nfils"), "left_outer") \
        .where(col("branch.cdepaysiso4npere").isNull()) \
        .select(col("leaves.cdepaysiso4nfils").alias("id_leaves"))

    # Create a DataFrame for territories
    territories = included_territory.alias("it").join(leaves, leaves.id_leaves == col("it.id_child")) \
        .groupBy("it.id").agg(collect_set("id_leaves").alias("cdepaysiso4n_fils"))

    # Add the leaves to the territories DataFrame
    territories = territories.union(
        leaves.groupBy("id_leaves").agg(collect_set("id_leaves").alias("cdepaysiso4n_fils")))

    return territories.toDF(*["cdepaysiso4n", "cdepaysiso4n_fils"])


def get_final_territories_by_idtercmplx(df_elt_ter, df_terr):
    @udf(returnType=ArrayType(IntegerType()))
    def rec_udf(ters) -> list:
        ters.sort()
        res = []
        for i in ters:
            if i[1]:
                res += i[2]
            else:
                res = list(set(res) - set(i[2]))
        return list(set(res))
    df = (
        df_elt_ter
        .join(df_terr, on="cdepaysiso4n")
        .groupBy(df_elt_ter.idtercmplx)
        .agg(rec_udf(array_agg(struct(df_elt_ter.ord_list,
                                      df_elt_ter.incl,
                                      df_terr.cdepaysiso4n_fils))).alias("territoires"))
        .select("idtercmplx", "territoires")
    )

    return df


def get_territories_computed_with_udf(df):
    def __modify_fils(cumulative_fils, incl_list):
        result_set = set()
        for fils, inc in zip(cumulative_fils, incl_list):
            if inc == 1:
                result_set.update(fils)
            elif inc == 0:
                result_set.difference_update(fils)
        return list(result_set)

    # Define the window specification for cumulate the countries
    window_spec = Window.partitionBy("idtercmplx").orderBy("ord_list").rowsBetween(Window.unboundedPreceding,
                                                                                   Window.currentRow)

    # Collect the cdepaysiso4n_fils values in an ordered list
    df = (df.withColumn("cumulative_fils", collect_list("cdepaysiso4n_fils").over(window_spec))
          .withColumn("incl_list", collect_list("incl").over(window_spec)))

    # Define UDF to handle inclusion and exclusion logic
    modify_fils_udf = udf(__modify_fils, returnType=ArrayType(LongType()))

    # Apply the UDF to get the final fils without duplicates
    df = df.withColumn("cdepaysiso4n_fils_list", modify_fils_udf("cumulative_fils", "incl_list"))

    # Define window to get the last row for each idtercmplx
    window_spec_last = Window.partitionBy("idtercmplx").orderBy("ord_list").rowsBetween(Window.unboundedPreceding,
                                                                                        Window.unboundedFollowing)

    # Use the last function to get the final cumulative result for each idtercmplx
    return (df.withColumn("cdepaysiso4n_fils_list", last("cdepaysiso4n_fils_list").over(window_spec_last))
            .dropDuplicates(["idtercmplx"])
            .select("idtercmplx", "cdepaysiso4n_fils_list")
            )

