import pytest
from pyspark.sql import SparkSession
import os


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.config("spark.driver.host", "127.0.0.1")
        .config(
            "spark.jars.packages",
            "org.streamstate:streamstate-utils_2.12:0.2.0-SNAPSHOT",
        )
        .master("local")
        .appName("tests")
        .getOrCreate()
    )
