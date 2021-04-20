from dataclasses import dataclass, field, is_dataclass
import marshmallow_dataclass

from marshmallow import Schema
from typing import ClassVar, Type, List, Dict


## Is output_name just the app_name?? that would make sense to me...
@dataclass
class OutputStruct:
    mode: str
    checkpoint_location: str  # this should almost certainly not be here....derive from the cluster
    output_name: str
    processing_time: str = "0"
    # primary_keys: List[str] = []  # not needed if dont persist to cassandra
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class TableStruct:
    primary_keys: List[str]
    # organization: str
    output_schema: dict  # avro schema
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class FileStruct:
    max_file_age: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class InputStruct:
    topic: str
    schema: dict  # partial avro schema, must include "fields"
    sample: List[dict] = field(default_factory=list)  # not all need a sample
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class CassandraInputStruct:
    cassandra_ip: str
    cassandra_port: str
    cassandra_password: str
    cassandra_user: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class CassandraOutputStruct:
    cassandra_cluster: str
    cassandra_key_space: str
    cassandra_table_name: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy


@dataclass
class KafkaStruct:
    brokers: str
    Schema: ClassVar[Type[Schema]] = Schema  # for mypy
