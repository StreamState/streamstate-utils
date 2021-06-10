from streamstate_utils.generic_wrapper import kafka_wrapper, file_wrapper
from streamstate_utils.run_test import helper_for_file
import pytest
from pyspark.sql import DataFrame, SparkSession
import os
import json
import shutil
from typing import List
import pyspark.sql.functions as F
from streamstate_utils.structs import InputStruct


def test_helper_for_file_succeeds_multiple_topics_and_rows(spark: SparkSession):

    windowedWordCounts = pairs.reduceByKeyAndWindow(
        lambda x, y: x + y, lambda x, y: x - y, 30, 10
    )

    def process(dfs: List[DataFrame]) -> DataFrame:
        [df1, df2] = dfs
        df1 = df1.withColumn("current_timestamp", F.current_timestamp()).withWatermark(
            "current_timestamp", "2 hours"
        )
        df2 = df2.withColumn("current_timestamp", F.current_timestamp()).withWatermark(
            "current_timestamp", "2 hours"
        )
        return df1.join(
            df2,
            (df1.field1 == df2.field1id)
            & (df1.current_timestamp >= df2.current_timestamp)
            & (
                df1.current_timestamp
                <= (df2.current_timestamp + F.expr("INTERVAL 1 HOURS"))
            ),
        ).select("field1", "value1", "value2")

    helper_for_file(
        "testhelpermultipletopics",
        "2d",
        ".",  # current folder
        process,
        [
            InputStruct(
                topic="topic1",
                topic_schema=[
                    {"name": "field1", "type": "string"},
                    {"name": "value1", "type": "string"},
                ],
                sample=[
                    {"field1": "somevalue", "value1": "hi1"},
                    {"field1": "somevalue1", "value1": "hi2"},
                ],
            ),
            InputStruct(
                topic="topic2",
                topic_schema=[
                    {"name": "field1id", "type": "string"},
                    {"name": "value2", "type": "string"},
                ],
                sample=[
                    {"field1id": "somevalue", "value2": "goodbye1"},
                    {"field1id": "somevalue1", "value2": "goodbye2"},
                ],
            ),
        ],
        spark,
        [
            {"field1": "somevalue", "value1": "hi1", "value2": "goodbye1"},
            {"field1": "somevalue1", "value1": "hi2", "value2": "goodbye2"},
        ],
    )
