import unittest

import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType

import barborapp.main

from pyspark_testcase import PySparkTestCase


class TestSetupConfig(unittest.TestCase):
    def test_sanity_test(self):
        barborapp.main.hello()
        self.assertTrue(1)


class TestPeriodStart(unittest.TestCase):
    def test_monday(self):
        self.assertEqual(barborapp.main._period_start("20200810"), "20200810")

    def test_last_year_friday(self):
        self.assertEqual(barborapp.main._period_start("20190628"), "20190624")


class TestCastColumns(PySparkTestCase):
    def test_string_to_date(self):
        # given
        inv_dt = ["2018-09-{:02d}".format(x) for x in range(1, 31)]
        df = self.spark.createDataFrame(inv_dt, StringType()).withColumnRenamed(
            "value", "inv_dt"
        )
        self.assertListEqual(df.dtypes, [("inv_dt", "string")])

        # when
        df = barborapp.main.cast_columns(
            df, ["inv_dt"], fill_value=None, type_=DateType()
        )

        # then
        self.assertListEqual((df.dtypes), [("inv_dt", "date")])

    def test_string_to_int_with_missing_columns(self):
        # given
        inv = [str(x) for x in range(1, 31)]
        df = self.spark.createDataFrame(inv, StringType()).withColumnRenamed(
            "value", "inv"
        )
        self.assertListEqual(df.dtypes, [("inv", "string")])

        # when
        df = barborapp.main.cast_columns(
            df, ["inv", "missing"], fill_value=0, type_="integer"
        )

        # then
        self.assertListEqual((df.dtypes), [("inv", "int"), ("missing", "int")])
        self.assertEqual(df.agg(F.sum("missing")).first()[0], 0)


class TestLoadData(PySparkTestCase):
    def test_with_sample_csv_data(self):
        data = barborapp.main.load_data(self.spark)
        self.assertEqual(len(data.columns), 21)
        self.assertSetEqual(
            set(data.columns),
            set(
                [
                    "order_type",
                    "order_revenue",
                    "order_source",
                    "order_profit",
                    "order_shop",
                    "order_packaging_fee",
                    "discount_amount",
                    "address_id",
                    "order_date",
                    "payment_source",
                    "days_since_last_order",
                    "order_size",
                    "order_delivery_fee",
                    "order_cost",
                    "discount_active",
                    "order_city",
                    "customer_id",
                    "order_id",
                    "period_start",
                    "first_order_active",
                    "dt",
                ]
            ),
        )


class TestAddMissingColumns(PySparkTestCase):
    def test_missing_columns(self):
        # given
        inv_dt = ["2018-09-{:02d}".format(x) for x in range(1, 31)]
        df = self.spark.createDataFrame(inv_dt, StringType()).withColumnRenamed(
            "value", "inv_dt"
        )
        self.assertSetEqual(set(df.columns), set(["inv_dt"]))

        # when
        df = barborapp.main._add_missing_columns(
            df, ["inv_dt", "cheese"], fill_value="ham", type_=StringType()
        )

        # then
        self.assertSetEqual(set(df.columns), set(["inv_dt", "cheese"]))
        self.assertEqual(df.first().cheese, "ham")

    def test_no_missing_columns(self):
        # given
        inv_dt = ["2018-09-{:02d}".format(x) for x in range(1, 31)]
        df = self.spark.createDataFrame(inv_dt, StringType()).withColumnRenamed(
            "value", "inv_dt"
        )
        self.assertSetEqual(set(df.columns), set(["inv_dt"]))

        # when
        df = barborapp.main._add_missing_columns(
            df, ["inv_dt"], fill_value="ham", type_=StringType()
        )

        # then
        self.assertSetEqual(set(df.columns), set(["inv_dt"]))
        self.assertNotEqual(df.first().inv_dt, "ham")
