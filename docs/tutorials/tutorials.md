# Tutorials Overview

Welcome to the Dagster ODP tutorials section! Here you'll find hands-on guides that will help you learn how to build data pipelines using Dagster ODP. The tutorials are designed to progressively introduce ODP concepts while building real-world data applications.

## Prerequisites

Before starting the tutorials, ensure you have:

- Python 3.9 or later installed
- Basic familiarity with Python and SQL
- Completed the [Quickstart Guide](../getting-started/quickstart.md)
- **Familiarity with Dagster concepts**, including:
    - Assets and asset dependencies
    - Jobs and job execution
    - Resources
    - Schedules and partitions
    - The Dagster UI

!!!tip "New to Dagster?"
    If you're new to Dagster, we recommend going through the [Dagster Quickstart](https://docs.dagster.io/getting-started/quickstart) and [Dagster Tutorial](https://docs.dagster.io/tutorial) before starting these tutorials. Understanding Dagster's core concepts will help you better understand how ODP enhances and simplifies Dagster development.

## Available Tutorials

### [Weather Analysis Tutorial](weather_tutorial.md) (Introductory)
**Time estimate: 30-45 minutes**

Start here! This tutorial introduces core ODP concepts while building a pipeline that downloads and analyzes weather data. You'll learn:

- Basic project structure
- Resource configuration
- Custom task creation
- Workflow definition
- Asset dependencies

The pipeline you'll build:
- Downloads weather data and country information
- Loads data into DuckDB
- Performs basic data transformations using SQL
- Exports aggregated results to CSV

### [Chess Data Analysis Tutorial](chess_part_1.md) (Advanced)
**Time estimate: 2-3 hours**

!!!note
    We recommend completing the Weather Analysis Tutorial before starting this one, as it builds on concepts introduced there.

A comprehensive tutorial split into three parts that demonstrates how to build a production-grade data pipeline using multiple modern data tools:

**[Part 1: Initial Setup and Data Load](chess_part_1.md)**

- Setting up a Dagster ODP project
- Integrating with DLT for API data ingestion
- Understanding ODP's enhanced DLT functionality
- Managing configuration across multiple files

**[Part 2: Monthly Data Load and Quality Checks](chess_part_2.md)**

- Implementing time-based partitioning
- Setting up scheduled jobs
- Integrating with Soda for data quality checks
- Managing incremental data loads

**[Part 3: Data Transformations with DBT](chess_part_3.md)**

- Integrating DBT with ODP
- Creating incremental transformation models
- Managing DBT variables through ODP
- Exporting transformed data

## Getting Help

If you encounter issues while working through the tutorials:

1. Check the error messages in the Dagster UI - they often point directly to the problem
2. Refer to the [Concepts](../concepts/concepts.md) section for detailed explanations
3. Visit the [Dagster Community](https://dagster.io/community) for additional support

Ready to begin? Start with the [Weather Analysis Tutorial](weather_tutorial.md)!