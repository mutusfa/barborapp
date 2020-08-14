import unittest

from pyspark.sql import SparkSession


class PySparkTestCase(unittest.TestCase):
    """Stolen from Cambridge Spark

    https://blog.cambridgespark.com/unit-testing-with-pyspark-fb31671b1ad8
    """

    @classmethod
    def create_testing_pyspark_session(cls):
        return (
            SparkSession.builder.master("local[2]")
            .appName("my-local-testing-pyspark-context")
            .getOrCreate()
        )

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
