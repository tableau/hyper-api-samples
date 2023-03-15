from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SqlType, TableDefinition, TableName, Nullability, Inserter, escape_string_literal

ORDERS_DATASET_S3 = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_small.csv")

# We need to manually enable S3 connectivity as this is still an experimental feature
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    # Create a connection to the Hyper process - we do not connect to a database
    with Connection(endpoint=hyper.endpoint) as connection:
        
        # Use the CREATE TEMP EXTERNAL TABLE syntax - this allows us to use the CSV file like a normal table name in SQL queries
        # We specify empty credentials as the bucket is publicy accessible; this may be different when used with your own data
        create_external_table = f"""
            CREATE TEMP EXTERNAL TABLE orders(
               order_date DATE, 
               product_id TEXT, 
               category TEXT,
               sales DOUBLE PRECISION
            )
            FOR S3_LOCATION({ORDERS_DATASET_S3}, 
                            ACCESS_KEY_ID => '',
                            SECRET_ACCESS_KEY => '',
                            REGION => 'us-west-2')
            WITH (FORMAT => 'csv', HEADER => true)
        """
        # Create the external table using `execute_command` which sends an instruction to the database - we don't expect a result value
        connection.execute_command(create_external_table)
        
        # Select the total sales amount per category from the external table
        query = f"""SELECT category, SUM(sales) 
                    FROM orders
                    GROUP BY category"""

        # Execute the query with `execute_list_query` as we expect multiple rows (one row per category) and two columns (category name and sum of sales)
        result = connection.execute_list_query(query)

        # Iterate over all rows in the result and print the category name and the sum of sales for that category
        for row in result:
            print(f"{row[0]}: {row[1]} USD")
