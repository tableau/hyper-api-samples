# parquet-to-hyper

## __parquet_to_hyper__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

These samples show you how Hyper can natively interact with S3 compatible services, without the need to install any external dependencies like `google-cloud-bigquery`.

# Get started

## __Prerequisites__

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Python 3.9+

- install the dependencies from the `requirements.txt` file

## Run the samples

The following instructions assume that you have set up a virtual environment for Python. For more information on
creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html)
in the Python Standard Library.

1. Open a terminal and activate the Python virtual environment (`venv`).

1. Navigate to the folder where you installed the samples.

1. Then follow the steps to run one of the samples which are shown below.

**Live query against a `.parquet` file which is stored on Google Storage**

Run the Python script

```bash
$ python query-parquet-on-gs.py 
```
This script will perform a live query on the Parquet file which is stored in this public Google Storage bucket: `gs://cloud-samples-data/bigquery/us-states/us-states.parquet`.

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)

- [Tableau Hyper API Reference (Python)](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/index.html)

- [The EXTERNAL function in the Hyper API SQL Reference](https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/functions-srf.html#FUNCTIONS-SRF-EXTERNAL)

