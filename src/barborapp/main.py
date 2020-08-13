from datetime import datetime
import logging
from typing import List, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import DataType

from barborapp.typing import AMOUNT_TYPE
from barborapp import aggregates


def hello():
    print("Hi there")


LOG = logging.getLogger(__name__)

# Utils ========================================================================


def rename_and_fill_columns(
    df: DataFrame, rename: dict, fill_value=None, add_missing_columns=True,
) -> DataFrame:
    if add_missing_columns:
        for column in rename:
            try:
                df[column]  # just to raise an exception if column doesn't exist
            except AnalysisException:
                df = df.withColumn(column, F.lit(fill_value))
    df = df.na.fill(fill_value)
    columns_to_keep = [
        col_name for col_name in df.columns if col_name not in rename
    ]
    select_args = columns_to_keep + [
        F.col(orig).alias(new) for orig, new in rename.items()
    ]
    return df.select(select_args)


def cast_columns(
    dataframe: DataFrame, columns: List[str], type_: Union[DataType, str]
) -> DataFrame:
    for col_name in columns:
        try:
            dataframe = dataframe.withColumn(
                col_name, F.col(col_name).cast(type_)
            )
        except AnalysisException:
            LOG.warning("%s column not found", col_name)
    return dataframe


# Loading of data ==============================================================


def _period_start(date_str):
    """Returns Monday of the week that's in date."""
    date = datetime.strptime(date_str, "%Y%m%d")
    week_number = date.strftime("%W")
    year = date.year
    week_start = datetime.strptime(f"{year}-{week_number}-1", "%Y-%W-%w")
    return week_start.strftime("%Y%m%d")


period_start = F.udf(_period_start)


def add_periods(dataframe: DataFrame, date_col: str = "dt") -> DataFrame:
    return dataframe.withColumn("period_start", period_start(date_col))


def load_data(
    spark: SparkSession, file_path: str = "sample_daily_data.csv"
) -> DataFrame:
    df = spark.read.csv(file_path, header=True)
    amount_column_names = [
        "order_revenue",
        "order_cost",
        "order_profit",
        "order_packaging_fee",
        "order_delivery_fee",
        "discount_amount",
    ]
    df = cast_columns(df, amount_column_names, AMOUNT_TYPE)
    int_columns = [
        "order_size",
        "discount_active",
        "address_id",
        "days_since_last_order",
        "first_order_active",
    ]
    df = cast_columns(df, int_columns, "integer")
    df = add_periods(df, "dt")
    return df


# Aggregation ==================================================================


def get_weekly_aggregates(dataframe: DataFrame) -> DataFrame:
    from importlib import reload

    reload(aggregates)
    grouped_data = dataframe.groupby(["customer_id", "period_start"])
    weekly_summary_df = grouped_data.count().select(
        ["customer_id", "period_start"]
    )
    scaffolding = weekly_summary_df.select("*")

    avgs_to_get = [
        "order_size",
        "order_revenue",
        "discount_amount",
        "order_delivery_fee",
        "order_packaging_fee",
    ]
    avgs = {}
    for column in avgs_to_get:
        try:
            avgs[column] = aggregates.get_avg(
                grouped_data, column, cast_type=AMOUNT_TYPE
            )
        except AnalysisException:
            avgs[column] = scaffolding.withColumn(column, F.lit(0))
    for column, df in avgs.items():
        weekly_summary_df = weekly_summary_df.join(
            df, ["customer_id", "period_start"]
        )

    sums_to_get = ["order_revenue", "order_profit"]
    sums = {}
    for column in sums_to_get:
        try:
            sums[column] = aggregates.get_sum(
                grouped_data, column, cast_type=AMOUNT_TYPE
            )
        except AnalysisException:
            sums[column] = scaffolding.withColumn(column, F.lit(0))
    for column, df in sums.items():
        weekly_summary_df = weekly_summary_df.join(
            df, ["customer_id", "period_start"]
        )

    counts_by_order_type = aggregates.get_count_by_order_type(grouped_data)
    weekly_summary_df.join(
        counts_by_order_type, ["customer_id", "period_start"]
    )

    avgs_to_get_by_order_type = ["order_revenue", "order_profit"]
    avgs_by_order_type = {
        column: aggregates.get_avg_by_order_type(grouped_data, column)
        for column in avgs_to_get_by_order_type
    }
    for column, df in avgs_by_order_type.items():
        avgs_by_order_type[column] = rename_and_fill_columns(
            df,
            {
                "Delivered": f"{column}_executed_avg",
                "Placed": f"{column}_placed_avg",
                "Returned": f"{column}_returned_avg",
            },
            fill_value=0,
        )
    for column, df in avgs_by_order_type.items():
        weekly_summary_df = weekly_summary_df.join(
            df, ["customer_id", "period_start"]
        )

    # Assuming description for order_size_*_sum in task is incorrect and instead
    # of amounts, numbers of products ordered are wanted

    sums_to_get_by_order_type = ["order_size"]
    sums_by_order_type = {
        column: aggregates.get_sum_by_order_type(grouped_data, column)
        for column in sums_to_get_by_order_type
    }
    for column, df in sums_by_order_type.items():
        sums_by_order_type[column] = rename_and_fill_columns(
            df,
            {
                "Delivered": f"{column}_executed_sum",
                "Placed": f"{column}_placed_sum",
                "Returned": f"{column}_returned_sum",
            },
            fill_value=0,
        )
    for column, df in sums_by_order_type.items():
        weekly_summary_df = weekly_summary_df.join(
            df, ["customer_id", "period_start"]
        )

    return weekly_summary_df
