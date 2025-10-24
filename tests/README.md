# Testing
You can get code coverage of the unit tests by adding `--cov` 
i.e. `python -m pytest tests/unit --cov --cov-report term-missing`

## Before running
Some tests run against a cluster, so a few things need to be configured:
1. By default, the tests will run on serverless clusters, but if you want to run the, on your databricks cluster, you will need to set cluster id. To set cluster id run `export DATABRICKS_CLUSTER_ID=<your cluster id>`
2. Ensure databricks connect is the same version as your cluster `pip install databricks-connect=<your version>`. By default latest databricks-connect version is installed in the container, which might not necessarily be the LTS version.

## Env configuration (if you're not using a docker container)
To run the unit tests locally, firstly the virtual environment needs to be setup (only required if you're not using a docker container).

Run the following commands to setup the venv (if you're not using a docker container):

```bash
    python -m venv venv
    source venv/bin/activate
    pip3 install -r .devcontainer/requirements.txt
```
To run the unit tests

1. Ensure your venv is activated by running (if using virtual environment) `source venv/bin/activate` 
2. To now run the unit test `python -m pytest tests/unit/`


