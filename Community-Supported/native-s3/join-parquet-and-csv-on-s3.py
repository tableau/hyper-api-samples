from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SqlType, TableDefinition, TableName, Nullability, Inserter, escape_string_literal

ORDERS_DATASET_2018 = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_2018.parquet")
ORDERS_DATASET_2019 = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_2019.parquet")
ORDERS_DATASET_2020 = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_2020.parquet")
ORDERS_DATASET_2021 = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_2021.parquet")

# CSV file which contains the orders that were returned by the customers
RETURNS_DATASET = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/returns.csv")

EMPTY_S3_CREDENTIALS = "ACCESS_KEY_ID => '', SECRET_ACCESS_KEY => ''"

# We need to manually enable S3 connectivity as this is still an experimental feature
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    # Create a connection to the Hyper process - we do not connect to a database
    with Connection(endpoint=hyper.endpoint) as connection:

        # We use the `ARRAY` syntax in the CREATE TEMP EXTERNAL TABLE statement to specify multiple files to be unioned
        create_ext_orders_table = f"""
            CREATE TEMP EXTERNAL TABLE orders
            FOR ARRAY[ S3_LOCATION({ORDERS_DATASET_2018}, {EMPTY_S3_CREDENTIALS}, REGION => 'us-west-2'),
                       S3_LOCATION({ORDERS_DATASET_2019}, {EMPTY_S3_CREDENTIALS}, REGION => 'us-west-2'),
                       S3_LOCATION({ORDERS_DATASET_2020}, {EMPTY_S3_CREDENTIALS}, REGION => 'us-west-2'),
                       S3_LOCATION({ORDERS_DATASET_2021}, {EMPTY_S3_CREDENTIALS}, REGION => 'us-west-2')]
            WITH (FORMAT => 'parquet')
        """
        connection.execute_command(create_ext_orders_table)

        # Create the `returns` table also as EXTERNAL TABLE
        create_ext_returns_table = f"""
            CREATE TEMP EXTERNAL TABLE returns(
                returned TEXT,
                order_id TEXT
            )
            FOR S3_LOCATION({RETURNS_DATASET}, {EMPTY_S3_CREDENTIALS}, REGION => 'us-west-2')
            WITH (FORMAT => 'csv', HEADER => 'true', DELIMITER => ';')
        """
        connection.execute_command(create_ext_returns_table)

        # Select the total sales amount per category from the CSV file
        # and drill down by whether the orders were returned or not
        query = f"""SELECT category,
                           (CASE WHEN returned IS NULL THEN 'Not Returned' ELSE 'Returned' END) AS return_info,
                           SUM(sales) 
                    FROM orders
                    LEFT OUTER JOIN returns on orders.order_id = returns.order_id
                    GROUP BY 1, 2
                    ORDER BY 1, 2"""

        # Execute the query with `execute_list_query`
        result = connection.execute_list_query(query)
       
        # Iterate over all rows in the result and print them
        print(f"{'Category':<20} {'Status':<20} Sales")
        print(f"{'--------':<20} {'------':<20} -----")
        for row in result:
            print(f"{row[0]:<20} {row[1]:<20} {row[2]:,.2f} USD")
