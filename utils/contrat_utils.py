import itertools

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType

df_cedant = "df_cedant"
df_cess = "df_cess"


def __flatten_list(nested_list):
    if not isinstance(nested_list, list):
        return []
    flattened = []
    for sublist in nested_list:
        if isinstance(sublist, list):
            flattened.extend(sublist)
        else:
            flattened.append(sublist)
    return flattened


@F.pandas_udf(ArrayType(IntegerType()))
def flatten_array_int(arr_col: pd.Series) -> pd.Series:
    def flatten(arr):
        if arr is None:
            return []
        return list(itertools.chain.from_iterable(arr))

    return arr_col.apply(flatten)

@F.pandas_udf(ArrayType(StringType()))
def flatten_array_str(arr_col: pd.Series) -> pd.Series:
    def flatten(arr):
        if arr is None:
            return []
        return list(itertools.chain.from_iterable(arr))

    return arr_col.apply(flatten)


flatten_int_list_udf = F.udf(__flatten_list, ArrayType(IntegerType()))
flatten_str_list_udf = F.udf(__flatten_list, ArrayType(StringType()))


def __get_udf(column):
    if column in ["numctr", "territoires", "restr_numcatal_include", "restr_numcatal_exclude"]:
        return flatten_int_list_udf
    else:
        return flatten_str_list_udf


def collect_distinct(column: str) -> Column:
    return F.array_distinct(collect_union(column)).alias(column)


def collect_intersect(column: str) -> Column:
    flatten_udf = __get_udf(column)
    return (F.array_intersect(flatten_udf(F.collect_list(F.col(f"{df_cedant}.{column}"))),
                              flatten_udf(F.collect_list(F.col(f"{df_cess}.{column}")))).alias(column))


def collect_union(column: str) -> Column:
    flatten_udf = __get_udf(column)
    return (flatten_udf(F.array_union(F.collect_list(f"{df_cedant}.{column}"), F.collect_list(f"{df_cess}.{column}")))
            .alias(column))


def compute_percentage(column: str, category: Column = None) -> Column:
    if column == "pct_editeur":
        return (
            F.when(category == "E",
                   (F.first(f"{df_cedant}.{column}") * F.first(f"{df_cess}.{column}") / 100).alias(column)
                   ).otherwise(F.first(f"{df_cedant}.{column}")).alias(column)
        )
    else:
        return F.first(f"{df_cedant}.{column}").alias(column)


def intersect_restr(col1: str, col2: str) -> F.col:
    fill = "empty" if "restr_genre" in col1 else -1
    col = F.when(
            (F.size(col1) == 0) & (F.size(col2) == 0),
            F.array()
    ).otherwise(
        F.when(
            ((F.size(col1) == 0) & (F.size(col2) > 0)) |
            ((F.size(col2) == 0) & (F.size(col1) > 0)) |
            (F.size(F.array_intersect(col1, col2)) == 0),
            F.array(F.lit(fill))
        ).otherwise(
            F.array_intersect(col1, col2)
        )
    )

    return col