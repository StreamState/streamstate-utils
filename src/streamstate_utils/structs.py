from dataclasses import dataclass, field, is_dataclass
import marshmallow_dataclass

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
    checkpoint_location: str  # this should almost certainly not be here....derive from the cluster
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
class CassandraInputStruct:
    """
    Data class for connecting to cassandra
    ...

    Attributes
    ----------
    cassandra_ip: ip address/hostname of the cassandra database
    cassandra_port: port of the cassandra database
    cassandra_password: password
    cassandra_user: user
    """

    cassandra_ip: str
    cassandra_port: str
    cassandra_password: str
    cassandra_user: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class CassandraOutputStruct:
    """
    Data class for writing to cassandra
    ...

    Attributes
    ----------
    cassandra_cluster: cluster name
    cassandra_key_space: keyspace to write to
    cassandra_table_name: table to write to
    """

    cassandra_cluster: str
    cassandra_key_space: str
    cassandra_table_name: str
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
