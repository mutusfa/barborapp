from typing import Union

from pyspark.sql import DataFrame, GroupedData
import pyspark.sql.functions as F
from pyspark.sql.types import DataType, StringType


# Utils ========================================================================

# I haven't found a way to find most repeating value in column if data is
# pregrouped
def mode(
    dataframe: DataFrame, aggregate_by: list, column: str, alias: str = None
) -> DataFrame:
    """Credits to bjack3 https://stackoverflow.com/a/36695251"""
    alias = alias or f"mode({column})"
    counts = dataframe.groupBy(aggregate_by + [column]).count().alias("counts")
    results = (
        counts.groupBy(aggregate_by)
        .agg(F.max(F.struct(F.col("count"), F.col(column))).alias("mode"))
        .select(*aggregate_by, f"mode.{column}")
    )
    return results.withColumnRenamed(column, alias)


# ==============================================================================


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


# ==============================================================================


def get_discount_active(grouped_data: GroupedData) -> DataFrame:
    return grouped_data.agg(
        F.sum("discount_active")
        .cast("boolean")  # force values of 0 and 1
        .cast("integer")
        .alias("discount_active")
    )


# Addresses ====================================================================


def get_last_address(grouped_data: GroupedData) -> DataFrame:
    return grouped_data.agg(F.last("address_id").alias("last_address"))


# Fees =========================================================================


def get_order_count_with_delivery_fee(grouped_data: GroupedData) -> DataFrame:
    return grouped_data.agg(
        F.count(F.when(F.col("order_delivery_fee") > 0, 1)).alias(
            "order_count_w_delivery_fee"
        )
    )


def get_order_count_with_packaging_fee(grouped_data: GroupedData) -> DataFrame:
    return grouped_data.agg(
        F.count(F.when(F.col("order_packaging_fee") > 0, 1)).alias(
            "order_count_w_packaging_fee"
        )
    )
