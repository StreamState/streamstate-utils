from streamstate_utils.pyspark_utils import (
    map_avro_to_spark_schema,
    get_folder_location,
)
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    FloatType,
)
import os


def test_map_avro_to_spark():
    fields = [
        {"name": "myfield1", "type": "int"},
        {"name": "myfield2", "type": "float"},
    ]
    result = map_avro_to_spark_schema(fields)
    assert result.fieldNames() == ["myfield1", "myfield2"]


def test_folder_location():
    app_name = "myapp"
    topic = "topic1"
    assert get_folder_location(app_name, topic) == "myapp/topic1"
