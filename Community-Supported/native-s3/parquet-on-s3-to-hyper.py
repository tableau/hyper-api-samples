from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SqlType, TableDefinition, TableName, Nullability, Inserter, escape_string_literal

# Details and license of dataset: https://registry.opendata.aws/nyc-tlc-trip-records-pds/
# NOTE: This dataset is currently not accessible - see above website for more details and to check if it has become available again
TAXI_DATASET = escape_string_literal("s3://nyc-tlc/trip%20data/yellow_tripdata_2021-06.parquet") # May release fixes a bug so that %20 doesn't need to be escaped manually
TAXI_DATASET_TABLE_NAME = "taxi_rides"
TAXI_DATASET_DBNAME = "taxi-rides-2021-06.hyper"
TAXI_DATASET_REGION = "us-east-1"

# Currently (last checked Aug 8, 2022) the NYC taxi dataset is not available on AWS OpenData, however access may get restored in the future
# Therefore, we're providing an alternative using our own orders data set in parquet format
ORDERS_DATASET = escape_string_literal("s3://hyper-dev-us-west-2-bucket/tc22-demo/orders_2018.parquet")
ORDERS_DATASET_TABLE_NAME = "orders"
ORDERS_DATASET_DBNAME = "orders-2018.hyper"
ORDERS_DATASET_REGION = "us-west-2"

# If AWS has restored access to the NYC taxi dataset, below config can be changed to reference the TAXI_DATASET when it becomes available again in the future
CURRENT_DATASET = ORDERS_DATASET
CURRENT_DATASET_TABLE_NAME = ORDERS_DATASET_TABLE_NAME
CURRENT_DATASET_DBNAME = ORDERS_DATASET_DBNAME
CURRENT_DATASET_REGION = ORDERS_DATASET_REGION

# We need to manually enable S3 connectivity as this is still an experimental feature
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters={"experimental_external_s3": "true"}) as hyper:
    # Create a connection to the Hyper process and let it create a database file - if it exists, it's overwritten
    with Connection(endpoint=hyper.endpoint, database=CURRENT_DATASET_DBNAME, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        
        # Use `TableName` so we do not have to worry about escaping in the SQL query we generate below
        # Note: This line does not create a table in Hyper, it just defines a name
        table_name = TableName("public", CURRENT_DATASET_TABLE_NAME)

		# Ingest the data from the parquet file into a Hyper Table
        # Since the schema is stored inside the parquet file, we don't need to specify it explicitly here  
        cmd = f"CREATE TABLE {table_name}" \
              f" AS ( SELECT * FROM EXTERNAL(S3_LOCATION({CURRENT_DATASET}, ACCESS_KEY_ID => '', SECRET_ACCESS_KEY => '', REGION => '{CURRENT_DATASET_REGION}')," \
              f"                             FORMAT => 'parquet'))"

        # We use `execute_command` to send the CREATE TABLE statement to Hyper
        # This may take some time depending on your network connectivity so AWS S3
        connection.execute_command(cmd)

        # Let's check how many rows we loaded
        row_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {table_name}")
        print (f"Loaded {row_count} rows")
