# Quickstart: Your First Dagster ODP Project

This quickstart guide will walk you through setting up a simple project that echoes "Hello, Dagster ODP!" to the console. By the end, you'll have a working Dagster ODP setup and understand the basics of creating assets.

## Prerequisites

- Python 3.9 or later
- pip (Python package installer)

## 1. Set Up Your Environment

Let's start by installing the necessary packages:

```bash
# Install Dagster ODP, preferably in a virtual environment
pip install git+https://github.com/runodp/dagster-odp.git
```

## 2. Create a Dagster Project

Use the Dagster CLI to scaffold a new project:

```bash
dagster project scaffold --name my-dagster-odp-project
cd my-dagster-odp-project
```

## 3. Set Up Dagster ODP Configuration

Create the necessary directories for Dagster ODP configuration:

```bash
mkdir -p odp_config/workflows
```

Now, create a file named `hello_world.yaml` in the `odp_config/workflows` directory with the following content:

```yaml title="hello_world.yaml"
assets:
  - asset_key: hello_world
    task_type: shell_command
    description: "A simple asset that echoes a greeting"
    params:
      command: echo "Hello, Dagster ODP!"
```

This defines an asset that runs a shell command to echo our greeting.

## 4. Configure Dagster Definitions

Replace the boilerplate code in `definitions.py` within the `my_dagster_odp_project` directory with the following content:

```python title="definitions.py"
from dagster_odp import build_definitions

defs = build_definitions("odp_config")
```

This builds the Dagster definitions using the config files in the `odp_config` directory.

## 5. Run Dagster Dev Server

Start the Dagster UI and daemon:

```bash
dagster dev
```

Open your browser and navigate to `http://localhost:3000`. You should see your Dagster instance with the `hello_world` asset.

## 6. Materialize the Asset

In the Dagster UI:

1. Navigate to the "Assets" tab and click on "Global Asset Lineage"
2. Find the `hello_world` asset
3. Click the "Materialize" button

This will run the shell command and echo "Hello, Dagster ODP!" in the logs.

## What's Next?

Congratulations! You've set up your first Dagster ODP project. Here are some next steps to deepen your understanding:

- [Tutorials](../tutorials/tutorials.md): Follow our step-by-step tutorials to build more complex workflows.
- [Concepts](../concepts/concepts.md): Explore the key concepts behind Dagster ODP.
- [Reference](../reference/reference.md): Comprehensive details on all features and configurations.
