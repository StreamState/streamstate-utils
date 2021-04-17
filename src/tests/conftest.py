import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.config("spark.driver.host", "127.0.0.1")
        .master("local")
        .appName("tests")
        .getOrCreate()
    )
