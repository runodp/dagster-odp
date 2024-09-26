import json

import pytest
import yaml

from dagster_vayu.config_manager.builders.base_builder import BaseBuilder


class ConcreteBuilder(BaseBuilder):
    def load_config(self, config_data, config_path):
        self.config = config_data or {}

    def get_config(self):
        return self.config


def test_singleton_behavior():
    builder1 = ConcreteBuilder(config_data={"key1": "value1"})
    builder2 = ConcreteBuilder(config_data={"key2": "value2"})

    assert builder1 is builder2
    assert builder1.get_config() == {"key1": "value1"}


def test_merge_configs():
    builder = ConcreteBuilder()
    merged_config = {
        "list_field": [1, 2],
        "dict_field": {"a": 1, "b": 2},
        "simple_field": "old_value",
    }
    new_config = {
        "list_field": [3, 4],
        "dict_field": {"b": 3, "c": 4},
        "simple_field": "new_value",
    }

    builder._merge_configs(merged_config, new_config)

    assert merged_config == {
        "list_field": [1, 2, 3, 4],
        "dict_field": {"a": 1, "b": 3, "c": 4},
        "simple_field": "new_value",
    }


def test_read_config_file(tmp_path):
    builder = ConcreteBuilder()

    # Test JSON file
    json_file = tmp_path / "config.json"
    json_data = {"key": "value", "list": [1, 2, 3]}
    json_file.write_text(json.dumps(json_data))
    assert builder._read_config_file(json_file) == json_data

    # Test YAML file
    yaml_file = tmp_path / "config.yaml"
    yaml_data = {"key": "value", "list": [1, 2, 3]}
    yaml_file.write_text(yaml.dump(yaml_data))
    assert builder._read_config_file(yaml_file) == yaml_data

    # Test unsupported file format
    unsupported_file = tmp_path / "config.txt"
    unsupported_file.touch()
    with pytest.raises(ValueError, match="Unsupported file format: .txt"):
        builder._read_config_file(unsupported_file)
