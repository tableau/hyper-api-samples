# parquet-to-hyper
## __parquet_to_hyper__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

These samples show you how Hyper can natively interact with Amazon S3, without the need to install any external dependencies like boto or aws-cli.
They originate from the Tableau Conference 2022 Hands-on Training Use Hyper as your Cloud Lake Engine - you can [check out the slides here](https://mkt.tableau.com/tc22/sessions/live/428-HOT-D1_Hands-onUseTheHyperAPI.pdf).

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

**Create a `.hyper` file from parquet file on S3**
Run the Python script
```bash
$ python parquet-on-s3-to-hyper.py
```

This script will read the parquet file from `s3://nyc-tlc/trip%20data/yellow_tripdata_2021-06.parquet`, visit [AWS OpenData](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) for more details and license about the dataset and insert the records into a table named `taxi_rides` which is stored in a `.hyper` database file.

This database file can then directly be opened with Tableau Desktop or Tableau Prep or it can be published to Tableau Online and Tableau Server as shown in [this example](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/publish-hyper).

**Live query against a `.csv` file which is stored on AWS S3**
Run the Python script

```bash
$ python query-csv-on-s3.py
```

This script will perform a live query on the CSV file which is stored in this public S3 bucket: `s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_small.csv`.

**Live query with multiple `.parquet` and `.csv` files which are stored on AWS S3**
Run the Python script

```bash
$ python https://tableau.github.io/hyper-db/lang_docs/pyhttps://tableau.github.io/hyper-db/lang_docs/py
```

This script will perform a live query on multiple `.parquet` files which are stored on AWS S3. It shows how to use the [`ARRAY` syntax](https://tableau.github.io/hyper-db/docs/sql/external/) to union multiple `.parquet` files and how `.parquet` files can be joined together with `.csv` files - as you would expect from normal database tables stored inside a `.hyper` file.

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://tableau.github.io/hyper-db)

- [Tableau Hyper API Reference (Python)](https://tableau.github.io/hyper-db/lang_docs/py/index.html)

- [The EXTERNAL function in the Hyper API SQL Reference](https://tableau.github.io/hyper-db/docs/sql/external/)

- [AWS command line tools documentation](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html), e.g. if you want to download some of the sample files to your local machine and explore them
