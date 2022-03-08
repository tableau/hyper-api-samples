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
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    HyperException

def print_list(l):
    for e in l:
        print(e)


def run_hyper_query_external():
    """
    An example demonstrating how to use Hyper to read data directly from external sources.
    
    More information can be found here:
    https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/external-data-in-sql.html
    https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/sql-copy.html
    https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/sql-createexternaltable.html
    https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/functions-srf.html#FUNCTIONS-SRF-EXTERNAL
    """

    # Start the Hyper process.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Open a connection to the Hyper process. This will also create the new Hyper file.
        # The `CREATE_AND_REPLACE` mode causes the file to be replaced if it
        # already exists.
        with Connection(endpoint=hyper.endpoint,
                        database="output_file.hyper",
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            print("Scenario 1: Create a table from filtered parquet data with a calculated extra column")
            # This SQL command queries a parquet file directly and creates the table 'low_prio_orders' in Hyper.
            # The created table contains the data that is returned from the 'SELECT' part of the query. I.e., only
            # a selection of columns, a new calculated column 'clerk_nr' and only the rows with low order priority.
            command_1 = """CREATE TABLE low_prio_orders AS 
                           SELECT o_orderkey, o_custkey, o_totalprice, CAST(SUBSTRING(o_clerk from 7) AS int) as clerk_nr
                           FROM external('orders_10rows.parquet') 
                           WHERE o_orderpriority = '5-LOW'"""

            connection.execute_command(command_1)
            
            print("table content:")
            print_list(connection.execute_list_query("SELECT * FROM low_prio_orders"))
            print()
                
         
            print("\nScenario 2: Query multiple external data sources in one query.")
            # This query reads data from a parquet and a CSV file and joins it. Note that, for CSV files, the schema of the file
            # has to be provided and currently cannot be inferred form the file directly (see the `DESCRIPTOR` argument below).
            command_2 = """SELECT l_partkey, SUM(l_quantity) 
                           FROM external('orders_10rows.parquet') 
                               join external('lineitem.csv',
                                      COLUMNS => DESCRIPTOR(l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity float,
                                                l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date,
                                                l_shipinstruct text, l_shipmode text, l_comment text),
                                      DELIMITER => ',', FORMAT => 'csv', HEADER => false)
                                on o_orderkey = l_orderkey GROUP BY l_partkey 
                           ORDER BY l_partkey"""
            print("result:")
            print_list(connection.execute_list_query(command_2))
            print()
            
           
            print("Scenario 3: Query multiple CSV files that have the same schema in one go.")
            # Note that, for CSV files, the schema of the file has to be provided and currently cannot be inferred form the file directly.
            # (see the `DESCRIPTOR` argument below).
            command_3 = """SELECT * 
                           FROM external(ARRAY['lineitem.csv','lineitem_2.csv'],
                                         COLUMNS => DESCRIPTOR(l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity float,
                                                               l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date,
                                                               l_shipinstruct text, l_shipmode text, l_comment text),
                                         DELIMITER => ',', FORMAT => 'csv', HEADER => false)
                           ORDER BY l_orderkey"""
                           
            print("result:")
            print_list(connection.execute_list_query(command_3))



if __name__ == '__main__':
    run_hyper_query_external()
