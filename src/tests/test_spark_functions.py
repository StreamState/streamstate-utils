from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from streamstate_utils.spark_functions import standard_deviation, geometric_mean


def test_standard_deviation(spark: SparkSession):
    data = [
        ("val1", 1.0, 0.0),
        ("val1", 1.0, 0.0),
        ("val1", 1.0, 1.0),
        ("val1", 2.0, 1.0),
        ("val1", 2.0, 1.0),
        ("val1", 2.0, 1.0),
        ("val1", 3.0, 0.0),
        ("val1", 3.0, 0.0),
        ("val1", 3.0, 0.0),
        ("val1", 3.0, 0.0),
        ("val2", 1.0, 0.0),
        ("val2", 1.0, 0.0),
        ("val2", 1.0, 0.0),
        ("val2", 1.0, 1.0),
        ("val2", 1.0, 1.0),
    ]
    table = spark.createDataFrame(data, ["group", "label", "prediction"])

    result = (
        table.groupBy("group")
        .agg(standard_deviation(F.col("label")).alias("GeometricMean"))
        .collect()
    )

    expected = [("val1", 0.8755950357709131), ("val2", 0.0)]

    for row, (e1, e2) in zip(result, expected):
        assert row.asDict()["GeometricMean"] == e2


def test_geometric_mean(spark: SparkSession):
    data = [
        ("val1", 1.0, 0.0),
        ("val1", 1.0, 0.0),
        ("val1", 1.0, 1.0),
        ("val1", 2.0, 1.0),
        ("val1", 2.0, 1.0),
        ("val1", 2.0, 1.0),
        ("val1", 3.0, 0.0),
        ("val1", 3.0, 0.0),
        ("val1", 3.0, 0.0),
        ("val1", 3.0, 0.0),
        ("val2", 1.0, 0.0),
        ("val2", 1.0, 0.0),
        ("val2", 1.0, 0.0),
        ("val2", 1.0, 1.0),
        ("val2", 1.0, 1.0),
    ]
    table = spark.createDataFrame(data, ["group", "label", "prediction"])

    result = (
        table.groupBy("group")
        .agg(geometric_mean(F.col("label")).alias("GeometricMean"))
        .collect()
    )

    expected = [("val1", 1.9105460086999304), ("val2", 1.0)]

    for row, (e1, e2) in zip(result, expected):
        assert row.asDict()["GeometricMean"] == e2
