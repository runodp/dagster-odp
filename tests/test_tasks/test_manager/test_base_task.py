from unittest.mock import Mock

from dagster import AssetExecutionContext

from dagster_vayu.tasks.manager.base_task import BaseTask


class SimpleTask(BaseTask):
    def run(self):
        return {}


def test_initialize():
    # Setup
    mock_context = Mock(spec=AssetExecutionContext)
    mock_context.resources.resource1 = "test_resource"
    task = SimpleTask()

    # Execute
    task.initialize(mock_context, ["resource1"])

    # Assert
    assert task._context == mock_context
    assert task._resources == {"resource1": "test_resource"}
