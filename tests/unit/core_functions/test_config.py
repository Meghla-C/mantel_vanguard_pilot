import pytest
from unittest.mock import patch, MagicMock
from core_functions.Config import Config


@pytest.fixture
def mock_project_path():
    return "/mock/project/path"


@pytest.fixture
def mock_yaml_data():
    return {
        "entity1": {"key1": "value1", "key2": "value2"},
        "entity2": {"key3": "value3", "key4": "value4"},
    }


@patch("core_functions.Config.project_path")
@patch("core_functions.Config.read_yaml")
def test_get_config_from_yaml(
    mock_read_yaml, mock_project_path_func, mock_project_path, mock_yaml_data
):
    # Mock the project_path and read_yaml functions
    mock_project_path_func.return_value = mock_project_path
    mock_read_yaml.return_value = mock_yaml_data

    # Initialize the Config object
    yaml_config = "config.yaml"
    yaml_key = "entity1"
    key_name = "entity_name"
    config = Config(yaml_config, yaml_key, key_name)

    # Assert the retrieved configuration
    expected_config = {"entity_name": "entity1", "key1": "value1", "key2": "value2"}
    assert config.get_config() == expected_config


def test_key_not_found(mock_project_path, mock_yaml_data):
    with patch(
        "core_functions.Config.project_path", return_value=mock_project_path
    ), patch("core_functions.Config.read_yaml", return_value=mock_yaml_data):
        yaml_config = "config.yaml"
        yaml_key = "non_existent_key"
        key_name = "entity_name"

        with pytest.raises(
            KeyError, match=f"Key '{yaml_key}' not found in the specified config file."
        ):
            Config(yaml_config, yaml_key, key_name)


def test_key_name_none(mock_project_path, mock_yaml_data):
    with patch(
        "core_functions.Config.project_path", return_value=mock_project_path
    ), patch("core_functions.Config.read_yaml", return_value=mock_yaml_data):
        yaml_config = "config.yaml"
        yaml_key = "entity1"
        key_name = None

        with pytest.raises(
            KeyError,
            match="Key Name 'None' is None. A valid key name is required to add to the configuration.",
        ):
            Config(yaml_config, yaml_key, key_name)
