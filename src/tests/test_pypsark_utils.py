from streamstate_utils.pyspark_utils import map_avro_to_spark_schema
from streamstate_utils.structs import SchemaStruct


def test_map_avro_to_spark():
    fields = [
        SchemaStruct(name="myfield1", type="int"),
        SchemaStruct(name="myfield2", type="float"),
    ]
    result = map_avro_to_spark_schema(fields)
    assert result.fieldNames() == ["myfield1", "myfield2"]
