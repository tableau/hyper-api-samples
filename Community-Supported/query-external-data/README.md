# query-external-data

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

This sample demonstrates how you can use Hyper to query external data like parquet or CSV files directly. This enables a variety of ETL capabilities like accessing multiple files at once, filtering the read data and creating additional calculated columns.

# Get started

## __Prerequisites__

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Python 3.7 or newer

## Run the sample

Ensure that you have installed the requirements and then just run the sample Python file.
The following instructions assume that you have set up a virtual environment for Python. For more information on
creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html)
in the Python Standard Library.

1. Open a terminal and activate the Python virtual environment (`venv`).

1. Navigate to the folder where you installed the sample.

1. Run the Python script:
   
   **python query_external_data.py**

   It will read the `orders_10rows.parquet` file from the working directory and create a new Hyper database
   named `orders.hyper` with a table named "orders", which will contain the 10 rows copied from the Parquet file.

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)

- [Tableau Hyper API Reference (Python)](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/index.html)

- [The Hyper API SQL Reference](https://help.tableau.com/current/api/hyper_api/en-us/reference/sql)