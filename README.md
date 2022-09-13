# Prefect Workflows

This repo is to test and build workflows with **Prefect** and **Python** along with ```Prefect Cloud```.

## Followed Lectures

* [Prefect: How to Write and Schedule Your First ETL Pipeline with Python](https://towardsdatascience.com/prefect-how-to-write-and-schedule-your-first-etl-pipeline-with-python-54005a34f10b).
* [Prefect Tutorials](https://docs.prefect.io/tutorials).
* [Prefect ETL Tutorial (docs-v1)](https://docs-v1.prefect.io/core/tutorial/).

### Repo Init

1. ```pip install pipenv```.
2. ```pipenv install``` (the required packages are already defined in _pipfile/piplock_).

### Flow Run / Deployment

* If you want to run a workflow locally you just can type:
  * ```python [prefect_flow_file].py```.
* If you want to deploy the FLOW to prefect cloud:
    1. **Build**: ```prefect deployment build ./[python_file].py:[flow_name] -n [deployment_name] -q [queue_name]```. This command is going to build-up the Flow, and create **.prefectignore** + **[flow_name]-deployment.yaml** as configuration of the flow.
    2. **Create**: ```prefect deployment apply [flow_name]-deployment.yaml```.
    3. **Run**: ```prefect deployment run '[flow_name]/[deployment_name]'```.
    4. **Start Agent**: ```prefect agent start -q '[queue_name]'```.
