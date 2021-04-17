from streamstate_utils.utils import (
    get_folder_location,
    get_cassandra_table_name_from_app_name,
    map_avro_to_spark_schema,
    get_cassandra_outputs_from_config_map,
    get_cassandra_inputs_from_config_map,
)
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    FloatType,
)
import os


def test_folder_location():
    app_name = "myapp"
    topic = "topic1"
    assert get_folder_location(app_name, topic) == "myapp/topic1"


def test_get_cassandra_table_name_from_app_name():
    app_name = "myapp"
    version = "1"
    assert get_cassandra_table_name_from_app_name(app_name, version) == "myapp_1"


def test_map_avro_to_spark():
    fields = [
        {"name": "myfield1", "type": "int"},
        {"name": "myfield2", "type": "float"},
    ]
    result = map_avro_to_spark_schema(fields)
    assert result.fieldNames() == ["myfield1", "myfield2"]


def test_get_cassandra_inputs_from_config_map():
    os.environ["data_center"] = "dc"
    os.environ["cassandra_cluster_name"] = "ccn"
    os.environ["port"] = "9042"
    os.environ["username"] = "user"
    os.environ["password"] = "pass"
    result = get_cassandra_inputs_from_config_map()
    assert result.cassandra_ip == "ccn-dc-service"


def test_get_cassandra_outputs_from_config_map():
    os.environ["organization"] = "testorg"
    os.environ["cassandra_cluster_name"] = "ccn"
    result = get_cassandra_outputs_from_config_map("myapp", "1")
    assert result.cassandra_key_space == "testorg"
    assert result.cassandra_table_name == "myapp_1"
