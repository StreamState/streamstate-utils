from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List, Callable
from streamstate_utils.pyspark_utils import (
    map_avro_to_spark_schema,
)
from streamstate_utils.kafka_utils import get_kafka_output_topic_from_app_name
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


def kafka_wrapper(
    app_name: str,
    brokers: str,
    process: Callable[[List[DataFrame]], DataFrame],
    inputs: List[InputStruct],
    spark: SparkSession,
) -> DataFrame:
    dfs = [
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", input.topic)
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(
            F.from_json(
                F.col("json"), schema=map_avro_to_spark_schema(input.schema)
            ).alias("data")
        )
        .select("data.*")
        for input in inputs
    ]
    return process(dfs)


def file_wrapper(
    app_name: str,
    max_file_age: str,
    base_folder: str,
    process: Callable[[List[DataFrame]], DataFrame],
    inputs: List[InputStruct],
    spark: SparkSession,
) -> DataFrame:
    dfs = [
        spark.readStream.schema(map_avro_to_spark_schema(input.schema))
        .option("maxFileAge", max_file_age)
        .json(os.path.join(base_folder, get_folder_location(app_name, input.topic)))
        for input in inputs
    ]
    return process(dfs)


def write_kafka(batch_df: DataFrame, kafka: KafkaStruct, app_name: str, version: str):
    batch_df.write.format("kafka").option(
        "kafka.bootstrap.servers", kafka.brokers
    ).option("topic", get_kafka_output_topic_from_app_name(app_name, version)).save()


def write_parquet(batch_df: DataFrame, app_name: str, base_folder: str, topic: str):
    batch_df.write.format("parquet").option(
        "path", os.path.join(base_folder, get_folder_location(app_name, topic))
    ).save()


def write_firestore(
    batch_df: DataFrame, firestore: FirestoreOutputStruct, table: TableStruct
):
    batch_df.foreachPartition(
        apply_partition_hof(
            firestore.project_id,
            firestore.firestore_collection_name,
            firestore.version,
            table.primary_keys,
        )
    )


def write_console(
    result: DataFrame,
    checkpoint_location: str,
    mode: str,
):
    result.writeStream.format("console").outputMode("append").option(
        "truncate", "false"
    ).option("checkpointLocation", checkpoint_location).start().awaitTermination()


def write_wrapper(
    result: DataFrame,
    output: OutputStruct,
    checkpoint_location: str,
    write_fn: Callable[[DataFrame], None],
    # processing_time: str = "0",
):
    result.writeStream.outputMode(output.mode).option("truncate", "false").trigger(
        processingTime=output.processing_time
    ).option("checkpointLocation", checkpoint_location).foreachBatch(
        lambda df, id: write_fn(df)
    ).start().awaitTermination()
