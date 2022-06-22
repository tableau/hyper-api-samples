from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SqlType, TableDefinition, TableName, Nullability, Inserter, escape_string_literal

# Details and license of dataset: https://registry.opendata.aws/nyc-tlc-trip-records-pds/
TAXI_DATASET = escape_string_literal("s3://nyc-tlc/trip%20data/yellow_tripdata_2021-06.parquet") # May release fixes a bug so that %20 doesn't need to be escaped manually

# We need to manually enable S3 connectivity as this is still an experimental feature
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters={"experimental_external_s3": "true"}) as hyper:
    # Create a connection to the Hyper process and let it create a database file - if it exists, it's overwritten
    with Connection(endpoint=hyper.endpoint, database="taxi-rides-2021-06.hyper", create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        
        # Use `TableName` so we do not have to worry about escaping in the SQL query we generate below
        # Note: This line does not create a table in Hyper, it just defines a name
        taxi_rides = TableName("public", "taxi_rides")

		# Ingest the data from the parquet file into a Hyper Table
        # Since the schema is stored inside the parquet file, we don't need to specify it explicitly here
        cmd = f"CREATE TABLE {taxi_rides}" \
              f" AS ( SELECT * FROM EXTERNAL(S3_LOCATION({TAXI_DATASET}), FORMAT => 'parquet'))"

        # We use `execute_command` to send the CREATE TABLE statement to Hyper
        # This may take some time depending on your network connectivity so AWS S3
        connection.execute_command(cmd)

        # Let's check how many rows we loaded
        ride_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {taxi_rides}")
        print (f"Loaded {ride_count} taxi rides")
