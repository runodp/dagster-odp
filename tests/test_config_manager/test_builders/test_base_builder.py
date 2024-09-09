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
