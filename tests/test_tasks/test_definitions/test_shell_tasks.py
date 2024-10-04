from unittest.mock import Mock, patch

from dagster import AssetExecutionContext

from dagster_odp.tasks.definitions import ShellCommand


def test_shell_command():
    # Setup
    mock_context = Mock(spec=AssetExecutionContext)
    task = ShellCommand(command="echo 'Hello, World!'")
    task.initialize(mock_context)

    # Execute
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "Hello, World!\n"
        mock_run.return_value.returncode = 0
        result = task.run()

    # Assert
    assert result == {"command": "echo 'Hello, World!'", "return_code": 0}
    mock_run.assert_called_once()
