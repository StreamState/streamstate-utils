import os

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


def get_env_variables_from_config_map() -> dict:
    return {name: os.getenv(name, "") for name in ENV_NAMES}


# minorly inefficient
def get_organization_from_config_map() -> str:
    return get_env_variables_from_config_map()["organization"]


# minorly inefficient
def get_project_from_config_map() -> str:
    return get_env_variables_from_config_map()["project"]
