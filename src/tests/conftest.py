import pytest
from pyspark.sql import SparkSession
import os
from pathlib import Path
from single_source import get_version


@pytest.fixture(scope="session")
def spark():
    version = get_version(__name__, Path(__file__).parent)
    return (
        SparkSession.builder.config("spark.driver.host", "127.0.0.1")
        .config(
            "spark.jars.packages",
            f"org.streamstate:streamstate-utils_2.12:${version}",
        )
        .master("local")
        .appName("tests")
        .getOrCreate()
    )
