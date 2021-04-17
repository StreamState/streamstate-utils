from streamstate_utils.utils import (
    get_folder_location,
    get_cassandra_table_name_from_app_name,
    map_avro_to_spark_schema,
)
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    FloatType,
)


def test_folder_location():
    app_name = "myapp"
    topic = "topic1"
    assert get_folder_location(app_name, topic) == "myapp/topic1"


def test_get_cassandra_table_name_from_app_name():
    app_name = "myapp"
    version = "1"
    assert get_folder_location(app_name, version) == "myapp_1"


def test_map_avro_to_spark():
    fields = [
        {"name": "myfield1", "type": "int"},
        {"name": "myfield2", "type": "float"},
    ]
    result = map_avro_to_spark_schema(fields)
    assert result.fieldNames() == ["myfield1", "myfield2"]
