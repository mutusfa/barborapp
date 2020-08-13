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
    grouped_data = dataframe.groupby(["customer_id", "period_start"])
    weekly_summary_df = grouped_data.count().select(
        ["customer_id", "period_start"]
    )
    order_counts = aggregates.get_order_count_by_type(grouped_data)
    order_counts = rename_and_fill_columns(
        order_counts,
        {
            "Delivered": "order_executed_count",
            "Placed": "order_placed_count",
            "Returned": "order_returned_count",
        },
        fill_value=0,
    )
    order_counts.show()
    weekly_summary_df = weekly_summary_df.join(
        order_counts,
        (weekly_summary_df.customer_id == order_counts.customer_id)
        & (weekly_summary_df.period_start == order_counts.period_start),
    )
    return weekly_summary_df
