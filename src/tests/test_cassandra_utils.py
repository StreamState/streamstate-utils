from streamstate_utils.cassandra_utils import (
    get_cassandra_table_name_from_app_name,
    get_cassandra_outputs_from_config_map,
    get_cassandra_inputs_from_config_map,
)

import os


def test_get_cassandra_table_name_from_app_name():
    app_name = "myapp"
    version = "1"
    assert get_cassandra_table_name_from_app_name(app_name, version) == "myapp_1"


def test_get_cassandra_inputs_from_config_map():
    os.environ["data_center"] = "dc"
    os.environ["cassandra_cluster_name"] = "ccn"
    os.environ["port"] = "9042"
    os.environ["username"] = "user"
    os.environ["password"] = "pass"
    os.environ["spark_namespace"] = "sn"
    result = get_cassandra_inputs_from_config_map()
    assert result.cassandra_ip == "ccn-dc-service.sn.svc.cluster.local"


def test_get_cassandra_outputs_from_config_map():
    os.environ["organization"] = "testorg"
    os.environ["cassandra_cluster_name"] = "ccn"
    result = get_cassandra_outputs_from_config_map("myapp", "1")
    assert result.cassandra_key_space == "testorg"
    assert result.cassandra_table_name == "myapp_1"
