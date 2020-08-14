import unittest

from pyspark.sql.types import StringType

import barborapp.main

from pyspark_testcase import PySparkTestCase


class TestSetupConfig(unittest.TestCase):
    def test_sanity_test(self):
        barborapp.main.hello()
        self.assertTrue(1)


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
