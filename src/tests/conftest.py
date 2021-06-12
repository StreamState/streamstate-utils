import pytest
from pyspark.sql import SparkSession
from single_source import get_version
import pkg_resources


@pytest.fixture(scope="session")
def spark():
    version = pkg_resources.get_distribution("streamstate-utils").version
    return (
        SparkSession.builder.config("spark.driver.host", "127.0.0.1")
        .config(
            "spark.jars.packages",
            f"org.streamstate:streamstate-utils_2.12:{version}",
        )
        .master("local")
        .appName("tests")
        .getOrCreate()
    )
