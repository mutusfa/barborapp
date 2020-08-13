from typing import Union

from pyspark.sql import DataFrame, GroupedData
import pyspark.sql.functions as F
from pyspark.sql.types import DataType, StringType
from pyspark.sql.utils import AnalysisException

from barborapp.typing import AMOUNT_TYPE


# Utils ========================================================================


def mode(
    dataframe: DataFrame, group_by: str, mode_of: str, alias: str = None
) -> DataFrame:
    """Credits to bjack3 https://stackoverflow.com/a/36695251"""
    alias = alias or f"mode({mode_of})"
    counts = dataframe.groupBy([group_by, mode_of]).count().alias("counts")
    results = (
        counts.groupBy(group_by)
        .agg(F.max(F.struct(F.col("count"), F.col(mode_of))).alias("mode"))
        .select(group_by, f"mode.{mode_of}")
    )
    return results.withColumnRenamed(mode_of, alias)


# ==============================================================================


def get_count(
    grouped_data,
    column: str,
    alias: str = None,
    cast_type: Union[DataType, str] = StringType,
) -> DataFrame:
    alias = alias or f"{column}_count"
    return grouped_data.agg(F.count(column).cast(cast_type).alias(alias))


def get_sum(
    grouped_data,
    column: str,
    alias: str = None,
    cast_type: Union[DataType, str] = StringType,
) -> DataFrame:
    alias = alias or f"{column}_sum"
    return grouped_data.agg(F.sum(column).cast(cast_type).alias(alias))


def get_avg(
    grouped_data,
    column: str,
    alias: str = None,
    cast_type: Union[DataType, str] = StringType,
) -> DataFrame:
    alias = alias or f"{column}_avg"
    return grouped_data.agg(F.mean(column).cast(cast_type).alias(alias))


def get_count_by_order_type(grouped_data) -> DataFrame:
    return grouped_data.pivot("order_type").count().drop("order_type")


def get_avg_by_order_type(grouped_data, column: str,) -> DataFrame:
    return grouped_data.pivot("order_type").mean(column).drop("order_type")


def get_sum_by_order_type(grouped_data, column: str,) -> DataFrame:
    return grouped_data.pivot("order_type").sum(column).drop("order_type")


def get_discount_active(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(lambda x: x.discount_active > 0)
        .groupby("customer_id")
        .count()
    )


# Addresses ====================================================================


def get_last_address(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupby("customer_id").agg(
        F.last("address_id").alias("last_address")
    )


def get_top_address(dataframe: DataFrame) -> DataFrame:
    return mode(
        dataframe,
        group_by="customer_id",
        mode_of="address_id",
        alias="top_address",
    )


# ==============================================================================


def get_payment_source_top(dataframe: DataFrame) -> DataFrame:
    return mode(
        dataframe,
        group_by="customer_id",
        mode_of="payment_source",
        alias="payment_source_top",
    )


# Fees =========================================================================


def get_order_count_with_delivery_fee(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(dataframe.delivery_fee > 0)
        .groupby("customer_id")
        .count()
    )


def get_order_count_with_packaging_fee(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(dataframe.packaging_fee > 0)
        .groupby("customer_id")
        .count()
    )
