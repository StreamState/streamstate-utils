from streamstate_utils.firestore import (
    get_document_name_from_version_and_keys,
    get_firestore_inputs_from_config_map,
)

import os


def test_get_document_name_from_version_and_keys():
    pks = ["somevalue", "anothervalue"]
    version = "1"
    assert (
        get_document_name_from_version_and_keys(pks, version)
        == "somevalue_anothervalue_1"
    )


def test_get_firestore_inputs_from_config_map():
    os.environ["organization"] = "testorg"
    os.environ["project"] = "testproj"
    app_name = "testapp"
    version = "1"
    result = get_firestore_inputs_from_config_map(app_name, version)
    assert result.firestore_collection_name == "testorg_testapp"
    assert result.project_id == "testproj"
