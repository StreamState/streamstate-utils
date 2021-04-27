def get_kafka_output_topic_from_app_name(app_name: str, version: str) -> str:
    return f"{app_name}_{version}"
