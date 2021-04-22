from typing import Dict, Tuple
from streamstate_utils.structs import CassandraInputStruct, CassandraOutputStruct
import os


def get_folder_location(app_name: str, topic: str) -> str:
    return os.path.join(app_name, topic)


# get org name from ConfigMap
def get_cassandra_key_space_from_org_name(org_name: str) -> str:
    return org_name


def get_cassandra_table_name_from_app_name(app_name: str, version: str) -> str:
    return f"{app_name}_{version}"


ENV_NAMES = [
    "data_center",
    "cassandra_cluster_name",
    "port",
    "organization",
    "project",
    "org_bucket",
    "spark_namespace",
    "username",
    "password",
    # add checkpoint_location at some point
]


def _get_env_variables_from_config_map() -> dict:
    return {name: os.getenv(name, "") for name in ENV_NAMES}


def _convert_cluster_and_data_center_to_service_name(
    data_center: str, cassandra_cluster: str, namespace: str
) -> str:
    # service-x.namespace-b.svc.cluster.local
    return f"{cassandra_cluster}-{data_center}-service.{namespace}.svc.cluster.local"


def get_cassandra_inputs_from_config_map() -> CassandraInputStruct:
    env_var = _get_env_variables_from_config_map()
    return CassandraInputStruct(
        cassandra_ip=_convert_cluster_and_data_center_to_service_name(
            env_var["data_center"],
            env_var["cassandra_cluster_name"],
            env_var["spark_namespace"],
        ),
        cassandra_port=env_var["port"],
        cassandra_user=env_var["username"],
        cassandra_password=env_var["password"],
    )


# minorly inefficient
def get_organization_from_config_map() -> str:
    return _get_env_variables_from_config_map()["organization"]


def get_cassandra_outputs_from_config_map(
    app_name: str, version: str
) -> CassandraOutputStruct:
    env_var = _get_env_variables_from_config_map()
    return CassandraOutputStruct(
        cassandra_cluster=env_var["cassandra_cluster_name"],
        cassandra_key_space=get_cassandra_key_space_from_org_name(
            env_var["organization"]
        ),
        cassandra_table_name=get_cassandra_table_name_from_app_name(app_name, version),
    )
