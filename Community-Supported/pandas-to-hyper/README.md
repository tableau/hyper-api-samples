
# hyper-from-dataframe
## Create a Hyper File from a Pandas DataFrame

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

This Python script demonstrates how to create a `.hyper` file (Tableau's Hyper database format) from a pandas DataFrame. It uses Tableau's Hyper API to define a table structure, insert the data from the DataFrame, and save it as a `.hyper` file.

This is example shows an alternative to [using pantab](https://tableau.github.io/hyper-db/docs/guides/pandas_integration#loading-data-through-pandas), in case pantab cannot be used.
## Get Started

### Prerequisites

Before running the script, ensure you have the following installed:

- Python >= 3.6
- The required dependencies listed in `requirements.txt`.

### Install Dependencies

To install the necessary dependencies, run the following command:

```bash
pip install -r requirements.txt
```

### Running the Script

To run the script and generate the `.hyper` file, execute:

```bash
python create_hyper_from_pandas_dataframe.py
```

### What the Script Does

1. Creates a pandas DataFrame containing sample customer data.
2. Defines a table schema for the Hyper file, including columns like Customer ID, Customer Name, Loyalty Points, and Segment.
3. Inserts the DataFrame data into the Hyper file `customer.hyper`.
4. Verifies the number of rows inserted and prints a confirmation message.

### Modifying the Script

You can easily modify the script to load your own data by:

1. Changing the data inside the `data` dictionary to match your own structure.
2. Adjusting the table schema in the `TableDefinition` object accordingly to reflect your columns.

### Example Output

When you run the script, you should see output similar to this:

```
EXAMPLE - Load data from pandas DataFrame into table in new Hyper file
The number of rows in table Customer is 3.
The connection to the Hyper file has been closed.
The Hyper process has been shut down.
```

### Error Handling

If any issues occur, such as problems connecting to the Hyper file or inserting data, the script will raise an exception and print an error message to the console.

## Notes

This sample script demonstrates:

- How to use Tableau's `HyperProcess` and `Connection` classes.
- Defining table schemas using `TableDefinition`.
- Inserting data into the Hyper table using the `Inserter` class.

### Resources

- [Tableau Hyper API Documentation](https://tableau.github.io/hyper-db/lang_docs/py/index.html)
- [Tableau Hyper API SQL Reference](https://tableau.github.io/hyper-db/docs/sql/)
- [pandas Documentation](https://pandas.pydata.org/docs/)

