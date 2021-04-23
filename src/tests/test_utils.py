from streamstate_utils.utils import (
    get_folder_location,
)


def test_folder_location():
    app_name = "myapp"
    topic = "topic1"
    assert get_folder_location(app_name, topic) == "myapp/topic1"
