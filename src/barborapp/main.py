from datetime import datetime
import logging
from typing import List, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import DataType

from barborapp.typing import AMOUNT_TYPE


def hello():
    print("Hi there")


LOG = logging.getLogger(__name__)


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


def to_dates(dataframe: DataFrame, date_col: str = "dt") -> DataFrame:
    return dataframe.select(
        date_col, F.to_date(date_col, "yyyyMMdd").alias("date"),
    )


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
