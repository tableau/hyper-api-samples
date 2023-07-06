# parquet-to-hyper
## __parquet_to_hyper__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

This sample demonstrates how you can create a `.hyper` file from an Apache Parquet file.

# Get started

## __Prerequisites__

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Python 3.6 or 3.7

## Run the sample

Ensure that you have installed the requirements and then just run the sample Python file.
The following instructions assume that you have set up a virtual environment for Python. For more information on
creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html)
in the Python Standard Library.

1. Open a terminal and activate the Python virtual environment (`venv`).

1. Navigate to the folder where you installed the sample.

1. Run the Python script:
   
   **python create_hyper_file_from_parquet.py**

   It will read the `orders_10rows.parquet` file from the working directory and create a new Hyper database
   named `orders.hyper` with a table named "orders", which will contain the 10 rows copied from the Parquet file.

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://tableau.github.io/hyper-db)

- [Tableau Hyper API Reference (Python)](https://tableau.github.io/hyper-db/lang_docs/py/index.html)

- [The COPY command in the Hyper API SQL Reference](https://tableau.github.io/hyper-db/docs/sql/command/copy_from)
