from pyspark.sql import DataFrame, GroupedData, Window
import pyspark.sql.functions as F

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


def get_order_count_by_type(grouped_data: GroupedData) -> DataFrame:
    return grouped_data.pivot("order_type").count().drop("order_type")


# ===============================================================================


def get_order_size_avg(grouped_data: GroupedData) -> DataFrame:
    return grouped_data.agg(
        F.mean("order_size").cast("int").alias("order_size_avg")
    )


# ==============================================================================


def get_order_revenue_sum(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupby("customer_id").agg(
        F.sum("order_revenue").cast("decimal").alias("order_revenue_sum")
    )


def get_order_revenue_avg(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupby("customer_id").agg(
        F.mean("order_revenue").cast(AMOUNT_TYPE).alias("order_revenue_avg")
    )


def get_order_profit_by_type_avg(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.groupby(["customer_id"])
        .pivot("order_type")
        .mean("order_profit")
        .drop("order_type")
    )


# ==============================================================================


def get_discount_active(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(lambda x: x.discount_active > 0)
        .groupby("customer_id")
        .count()
    )


def get_discount_amount_avg(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupby("customer_id").agg(
        F.mean("discount_amount").cast(AMOUNT_TYPE).alias("discount_amount_avg")
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


def get_delivery_fee_avg(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupby("customer_id").agg(
        F.mean("delivery_fee").cast(AMOUNT_TYPE).alias("delivery_fee_avg")
    )


def get_order_count_with_delivery_fee(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(dataframe.delivery_fee > 0)
        .groupby("customer_id")
        .count()
    )


def get_packaging_fee_avg(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupby("customer_id").agg(
        F.mean("packaging_fee").cast(AMOUNT_TYPE).alias("packaging_fee_avg")
    )


def get_order_count_with_packaging_fee(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(dataframe.packaging_fee > 0)
        .groupby("customer_id")
        .count()
    )
