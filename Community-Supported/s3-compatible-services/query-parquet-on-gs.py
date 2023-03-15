"""Connect to Google Storage and query a parquet file located in a public bucket.

Adapted from hyper-api-samples/Community-Supported/native-s3/query-csv-on-s3.py
"""

from tableauhyperapi import Connection, HyperProcess, Telemetry, escape_string_literal

BUCKET_NAME = "cloud-samples-data"
FILE_PATH = "bigquery/us-states/us-states.parquet"

states_dataset_gs = escape_string_literal(
    f"s3://{BUCKET_NAME.strip('/')}/{FILE_PATH.strip('/')}"
)

# Hyper Process parameters
parameters = {}
# endpoint URL
parameters["external_s3_hostname"] = "storage.googleapis.com"
# We do not need to specify credentials and bucket location as the GS bucket is
# publicly accessible; this may be different when used with your own data

with HyperProcess(
    telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
    parameters=parameters,
) as hyper:
    # Create a connection to the Hyper process - we do not connect to a database
    with Connection(
        endpoint=hyper.endpoint,
    ) as connection:

        # Use the SELECT FROM EXTERNAL(S3_LOCATION()) syntax - this allows us to use
        # the parquet file like a normal table name in SQL queries
        sql_query = (
            f"""SELECT COUNT(*) FROM EXTERNAL(S3_LOCATION({states_dataset_gs}))"""
        )

        # Execute the query with `execute_scalar_query` as we expect a single number
        count = connection.execute_scalar_query(sql_query)
        print(f"number of rows : {count}")
