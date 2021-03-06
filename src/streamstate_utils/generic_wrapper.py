from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List, Callable
from streamstate_utils.pyspark_utils import (
    map_avro_to_spark_schema,
)
from streamstate_utils.kafka_utils import (
    get_kafka_output_topic_from_app_name,
    get_confluent_config,
)
from streamstate_utils.utils import get_folder_location
from streamstate_utils.structs import (
    OutputStruct,
    KafkaStruct,
    InputStruct,
    FirestoreOutputStruct,
    TableStruct,
)
from streamstate_utils.firestore import apply_partition_hof
import os


## TODO! provide consistent auth interface rather than hardcoding
## username and password
def kafka_wrapper(
    kafka: KafkaStruct,
    process: Callable[[List[DataFrame]], DataFrame],
    inputs: List[InputStruct],
    spark: SparkSession,
) -> DataFrame:
    """
    Read data from kafka
    ...

    Attributes
    ----------
    kafka: kafka parameters
    process: function to apply to dataframes
    inputs: for each topic, input parameters
    spark: the instantiated sparksession
    """
    confluent_config = get_confluent_config(kafka.brokers, prefix="kafka.")

    dfs = [
        spark.readStream.format("kafka")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("subscribe", input.topic)
        .options(**confluent_config)
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                kafka.confluent_api_key, kafka.confluent_secret
            ),
        )
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(
            F.from_json(
                F.col("json"), schema=map_avro_to_spark_schema(input.topic_schema)
            ).alias("data")
        )
        .select("data.*")
        for input in inputs
    ]
    return process(dfs).withColumn("topic_timestamp", F.current_timestamp())


def dev_file_wrapper(
    app_name: str,
    max_file_age: str,
    base_folder: str,
    process: Callable[[List[DataFrame]], DataFrame],
    inputs: List[InputStruct],
    spark: SparkSession,
) -> DataFrame:
    """
    Read data from json files for development
    ...

    Attributes
    ----------
    app_name: name of streaming application
    max_file_age: oldest file to include in streaming
    base_folder: base location to read from topic folders
    process: function to apply to dataframes
    inputs: for each topic, input parameters
    spark: the instantiated sparksession
    """
    return _file_wrapper(
        app_name, max_file_age, base_folder, process, inputs, spark, "json"
    )


## TODO, consider reading delta
def file_wrapper(
    app_name: str,
    max_file_age: str,
    base_folder: str,
    process: Callable[[List[DataFrame]], DataFrame],
    inputs: List[InputStruct],
    spark: SparkSession,
) -> DataFrame:
    """
    Read data from parquet files
    ...

    Attributes
    ----------
    app_name: name of streaming application
    max_file_age: oldest file to include in streaming
    base_folder: base location to read from topic folders
    process: function to apply to dataframes
    inputs: for each topic, input parameters
    spark: the instantiated sparksession
    """
    return _file_wrapper(
        app_name, max_file_age, base_folder, process, inputs, spark, "parquet"
    )


def _file_wrapper(
    app_name: str,
    max_file_age: str,
    base_folder: str,
    process: Callable[[List[DataFrame]], DataFrame],
    inputs: List[InputStruct],
    spark: SparkSession,
    format: str,
) -> DataFrame:
    dfs = [
        spark.readStream.schema(map_avro_to_spark_schema(input.topic_schema))
        .option("maxFileAge", max_file_age)
        .format(format)
        .load(os.path.join(base_folder, get_folder_location(app_name, input.topic)))
        for input in inputs
    ]
    return process(dfs)


def _write_file_wrapper(
    batch_df: DataFrame, app_name: str, base_folder: str, topic: str, format: str
):
    batch_df.write.mode("append").format(format).option(
        "path", os.path.join(base_folder, get_folder_location(app_name, topic))
    ).save()


def write_kafka(batch_df: DataFrame, kafka: KafkaStruct, app_name: str, version: str):
    """
    Write spark streaming back to Kafka
    ...

    Attributes
    ----------
    batch_df: dataframe to write
    kafka: kafka settings including key and secret
    app_name: name of streaming application
    version: code version as defined in FirestoreOutputStruct
    """
    confluent_config = get_confluent_config(kafka.brokers, prefix="kafka.")
    batch_df.select(F.to_json(F.struct(*batch_df.columns)).alias("value")).write.format(
        "kafka"
    ).options(**confluent_config).option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
            kafka.confluent_api_key, kafka.confluent_secret
        ),
    ).option(
        "topic", get_kafka_output_topic_from_app_name(app_name, version)
    ).save()


def write_json(batch_df: DataFrame, app_name: str, base_folder: str, topic: str):
    _write_file_wrapper(batch_df, app_name, base_folder, topic, "json")


## TODO, consider writing delta
def write_parquet(batch_df: DataFrame, app_name: str, base_folder: str, topic: str):
    """
    Write spark streaming to parquet
    ...

    Attributes
    ----------
    batch_df: dataframe to write
    app_name: name of streaming application
    base_folder: base location to create topic folders
    topic: topic name
    """
    _write_file_wrapper(batch_df, app_name, base_folder, topic, "parquet")


def write_firestore(
    batch_df: DataFrame, firestore: FirestoreOutputStruct, table: TableStruct
):
    """
    Write spark streaming to firestore
    ...

    Attributes
    ----------
    batch_df: dataframe to write
    firestore: firestore parameters
    table: table parameters
    """
    batch_df.foreachPartition(
        apply_partition_hof(
            firestore.project_id,
            firestore.firestore_collection_name,
            firestore.code_version,
            table.primary_keys,
        )
    )


def write_console(
    result: DataFrame,
    checkpoint_location: str,
):
    """
    Write spark streaming to console, to be used
    as a standalone streaming for debugging purposes
    ...

    Attributes
    ----------
    result: streaming dataframe
    checkpoint_location: checkpoint
    """
    result.writeStream.format("console").outputMode("append").option(
        "truncate", "false"
    ).option("checkpointLocation", checkpoint_location).start().awaitTermination()


def write_wrapper(
    result: DataFrame,
    output: OutputStruct,
    checkpoint_location: str,
    write_fn: Callable[[DataFrame], None],
):
    """
    Write spark streaming generically
    ...

    Attributes
    ----------
    result: streaming dataframe
    output: output parameters
    checkpoint_location: checkpoint
    write_fn: takes a batch dataframe, specifies the logic of the write
    """
    result.writeStream.outputMode(output.mode).option("truncate", "false").trigger(
        processingTime=output.processing_time
    ).option("checkpointLocation", checkpoint_location).foreachBatch(
        lambda df, id: write_fn(df)
    ).start().awaitTermination()
