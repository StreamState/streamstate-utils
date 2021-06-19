from streamstate_utils.kafka_utils import (
    get_kafka_output_topic_from_app_name,
    get_confluent_config,
)


def test_get_kafka_output_topic_from_app_name():
    app_name = "mytest"
    version = "1"
    assert get_kafka_output_topic_from_app_name(app_name, version) == "mytest_1"


def test_get_confluent_config_only_broker():
    brokers = "brokers"
    expected = {
        "bootstrap.servers": "brokers",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "ssl.endpoint.identification.algorithm": "https",
    }
    assert get_confluent_config(brokers) == expected


def test_get_confluent_config_broker_and_prefix():
    brokers = "brokers"
    prefix = "kafka."
    expected = {
        "kafka.bootstrap.servers": "brokers",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.ssl.endpoint.identification.algorithm": "https",
    }
    assert get_confluent_config(brokers, prefix=prefix) == expected


def test_get_confluent_config_broker_and_key_secret():
    brokers = "brokers"
    api_key = "api"
    secret = "secret"
    expected = {
        "bootstrap.servers": "brokers",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "ssl.endpoint.identification.algorithm": "https",
        "sasl.username": "api",
        "sasl.password": "secret",
    }
    assert get_confluent_config(brokers, api_key=api_key, secret=secret) == expected


def test_get_confluent_config_broker_and_key_secret_and_prefix():
    brokers = "brokers"
    api_key = "api"
    secret = "secret"
    prefix = "kafka."
    expected = {
        "kafka.bootstrap.servers": "brokers",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.ssl.endpoint.identification.algorithm": "https",
        "kafka.sasl.username": "api",
        "kafka.sasl.password": "secret",
    }
    assert (
        get_confluent_config(brokers, prefix=prefix, api_key=api_key, secret=secret)
        == expected
    )


def test_get_confluent_config_broker_and_key_and_prefix():
    brokers = "brokers"
    api_key = "api"
    prefix = "kafka."
    expected = {
        "kafka.bootstrap.servers": "brokers",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.ssl.endpoint.identification.algorithm": "https",
    }
    assert get_confluent_config(brokers, prefix=prefix, api_key=api_key) == expected
