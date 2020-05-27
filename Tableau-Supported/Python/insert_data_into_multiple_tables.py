# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------
from datetime import datetime
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException

# Table Definitions required to create tables
orders_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Orders",
    columns=[
        TableDefinition.Column(name="Address ID", type=SqlType.small_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Customer ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order Date", type=SqlType.date(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Ship Date", type=SqlType.date(), nullability=NULLABLE),
        TableDefinition.Column(name="Ship Mode", type=SqlType.text(), nullability=NULLABLE)
    ]
)

customer_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Customer",
    columns=[
        TableDefinition.Column(name="Customer ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Customer Name", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Loyalty Reward Points", type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Segment", type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)

products_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Products",
    columns=[
        TableDefinition.Column(name="Category", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Product ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Product Name", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Sub-Category", type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)

line_items_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Line Items",
    columns=[
        TableDefinition.Column(name="Line Item ID", type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Product ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Sales", type=SqlType.double(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Quantity", type=SqlType.small_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Discount", type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name="Profit", type=SqlType.double(), nullability=NOT_NULLABLE)
    ]
)


def run_insert_data_into_multiple_tables():
    """
    An example of how to create and insert data into a multi-table Hyper file where tables have different types
    """
    print("EXAMPLE - Insert data into multiple tables within a new Hyper file")
    path_to_database = Path("superstore.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "superstore.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            # Create multiple tables.
            connection.catalog.create_table(table_definition=orders_table)
            connection.catalog.create_table(table_definition=customer_table)
            connection.catalog.create_table(table_definition=products_table)
            connection.catalog.create_table(table_definition=line_items_table)

            # Insert data into Orders table.
            orders_data_to_insert = [
                [399, "DK-13375", datetime(2012, 9, 7), "CA-2011-100006", datetime(2012, 9, 13), "Standard Class"],
                [530, "EB-13705", datetime(2012, 7, 8), "CA-2011-100090", datetime(2012, 7, 12), "Standard Class"]
            ]

            with Inserter(connection, orders_table) as inserter:
                inserter.add_rows(rows=orders_data_to_insert)
                inserter.execute()

            # Insert data into Customers table.
            customer_data_to_insert = [
                ["DK-13375", "Dennis Kane", 518, "Consumer"],
                ["EB-13705", "Ed Braxton", 815, "Corporate"]
            ]

            with Inserter(connection, customer_table) as inserter:
                inserter.add_rows(rows=customer_data_to_insert)
                inserter.execute()

            # Insert individual row into Product table.
            with Inserter(connection, products_table) as inserter:
                inserter.add_row(row=["TEC-PH-10002075", "Technology", "Phones", "AT&T EL51110 DECT"])
                inserter.execute()

            # Insert data into Line Items table.
            line_items_data_to_insert = [
                [2718, "CA-2011-100006", "TEC-PH-10002075", 377.97, 3, 0.0, 109.6113],
                [2719, "CA-2011-100090", "TEC-PH-10002075", 377.97, 3, None, 109.6113]
            ]

            with Inserter(connection, line_items_table) as inserter:
                inserter.add_rows(rows=line_items_data_to_insert)
                inserter.execute()

            tables = [orders_table, customer_table, products_table, line_items_table]
            for table in tables:
                # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
                row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {table.table_name}")
                print(f"The number of rows in table {table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_insert_data_into_multiple_tables()
    except HyperException as ex:
        print(ex)
        exit(1)
