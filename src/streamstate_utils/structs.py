from dataclasses import dataclass, field  # , is_dataclass

# import marshmallow_dataclass

from marshmallow import Schema
from typing import ClassVar, Type, List, Dict


@dataclass
class OutputStruct:
    """
    Data class for output kafka topic.
    Note that the output topic name is created seperately from the app_name and version.
    ...

    Attributes
    ----------
    mode: streaming output mode, one of "append", "complete", "update"
    checkpoint_location: full path to checkpoint location in case spark job crashes.  This should be depricated and put into terraform directly
    processing_time: optional time to wait to aggregate inputs before writing
    """

    mode: str
    # checkpoint_location: str  # this should almost certainly not be here....derive from the cluster
    # output_name: str
    processing_time: str = "0"
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class TableStruct:
    """
    Data class for cassandra table.
    ...

    Attributes
    ----------
    primary_keys: list of keys that define the primary key in the cassandra table
    output_schema: list of dictionaries with name and type attributes describing the table's schema
    """

    primary_keys: List[str]
    output_schema: List[Dict[str, str]]  # name: "field1", type: "string"
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class FileStruct:
    """
    Data class for reading data from files/gcs.
    ...

    Attributes
    ----------
    max_file_age: how far back to go to retrieve data
    """

    max_file_age: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class InputStruct:
    """
    Data class for incoming kafka topics.
    ...

    Attributes
    ----------
    topic: incoming kafka topic
    schema: list of dictionaries with name and type attributes describing the kafka topic's schema
    sample: optional list of possible kafka topic payloads (used for running unit tests)
    """

    topic: str
    schema: List[Dict[str, str]]  # name: "field1", type: "string"
    sample: List[dict] = field(default_factory=list)  # not all need a sample
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class KafkaStruct:
    """
    Data class for connecting to kafka
    ...

    Attributes
    ----------
    brokers: comma seperated string of brokers
    """

    brokers: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class FirestoreOutputStruct:
    """
    Data class for writing to cassandra
    ...

    Attributes
    ----------
    firestore_collection_name: firestore collection
    firestore_document_name: firestore document
    project_id: gcp project
    version: schema version
    primary_keys: fields that represent primary keys in the data model
    """

    firestore_collection_name: str
    project_id: str
    version: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy
