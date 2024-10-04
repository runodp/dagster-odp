import shlex
import subprocess
from typing import Any, Dict, Optional

from dagster_odp.tasks.manager import BaseTask, odp_task


@odp_task("shell_command", compute_kind="shell")
class ShellCommand(BaseTask):
    """
    A task that runs a shell command using subprocess.

    Attributes:
        cmd (str): The command to run as a string.
        env (Optional[Dict[str, str]]): Environment variables to set for the command.
        cwd (Optional[str]): The working directory for the command.
    """

    command: str
    env_vars: Optional[Dict[str, str]] = None
    working_dir: Optional[str] = None

    def run(self) -> Dict[str, Any]:

        # Convert the string command to a list of arguments
        cmd_list = shlex.split(self.command)

        # Run the command and capture the output
        result = subprocess.run(
            cmd_list,
            env=self.env_vars,
            cwd=self.working_dir,
            capture_output=True,
            text=True,
            check=True,
        )

        # Log the result using Dagster's logging
        self._context.log.info(result.stdout)

        # Prepare the return dictionary
        output = {
            "command": self.command,
            "return_code": result.returncode,
        }

        return output
