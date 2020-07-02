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

""" Tool to adjust vertex order of polygons in a hyper file

This script enables a customer to adjust the vertex order of all polygons in a hyper file
It provides two commands, list and run
 - List: enumerates all tables specifying which columns are of 'GEOGRAPHY' type
 - Run: adjusts the vertex order of all polygons in a .hyper file, writing the output to a new .hyper file
   Run has two modes: auto and invert:
    - auto mode automatically adjusts the vertex order according to the interior-left definition of
      polygons assuming the data comes from a data source that uses a flat-earth topology
    - invert mode inverts the vertex order for all polygons
   All other (non-geography) columns are just copied as is to the output file
   Tables without geography columns are also copied to the output file
"""

import argparse
import sys

from enum import Enum
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, Connection, SqlType, TableDefinition, TableName, SchemaName


class ListTables:
    """ Command to list tables with spatial columns in a .hyper file"""

    Description = "Lists all tables in a .hyper file and shows columns of type GEOGRAPHY"
    """ Description of the command """

    def define_args(self, arg_parser):
        """ Adds arguments for the command
        :param arg_parser: The argparse.ArgumentParser to add arguments to
        """
        arg_parser.add_argument("-i", "--input_file", type=Path, metavar="<input.hyper>",
                                required=True, help="Input .hyper file")

    def run(self, args):
        """ Runs the command
        :param args: Arguments from argparse.Namespace
        """
        input_file = Path(args.input_file)
        print("Listing tables with spatial columns")

        # Starts the Hyper Process with telemetry enabled to send data to Tableau.
        # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Connects to existing Hyper file
            with Connection(endpoint=hyper.endpoint,
                            database=input_file) as connection:
                catalog = connection.catalog
                # Iterates over all schemas in the input file
                for schema_name in catalog.get_schema_names():
                    # Iterates over all tables in the current schema
                    for table in catalog.get_table_names(schema=schema_name):
                        table_definition = catalog.get_table_definition(name=table)
                        rows_in_table = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {table}")
                        spatial_columns = [c.name for c in table_definition.columns if c.type == SqlType.geography()]
                        if spatial_columns:
                            print(f"Table {table} with {rows_in_table} rows has"
                                  f" {len(spatial_columns)} spatial columns: {spatial_columns}")
                        else:
                            print(f"Table {table} with {rows_in_table} rows has no spatial columns")


class AdjustVertexOrderMode(Enum):
    """ Modes for adjusting vertex order """
    AUTO = "auto"
    INVERT = "invert"


class AdjustVertexOrder:
    """ Command to adjust vertex order of all polygons in a .hyper file """

    Description = "Copies tables from a .hyper file to a new file while adjusting vertex order of all polygons"
    """ Description of the command """

    def define_args(self, arg_parser):
        """ Adds arguments for the command
        :param arg_parser: The argparse.ArgumentParser to add arguments to
        """
        arg_parser.add_argument("-i", "--input_file", type=Path, metavar="<input.hyper>",
                                required=True, help="Input .hyper file")
        arg_parser.add_argument("-o", "--output_file", type=Path, metavar="<output.hyper>",
                                required=True, help="Output .hyper file")
        arg_parser.add_argument("-m", "--mode", type=AdjustVertexOrderMode, choices=list(AdjustVertexOrderMode),
                                required=True, help="Vertex order adjustment mode: "
                                "(auto | invert). Auto: assuming data comes "
                                "from a source with a flat - earth topology, "
                                "it automatically adjusts the vertex order according "
                                "to the interior - left definition of polygons. "
                                "Invert: inverts the vertex order for all polygons.")

    def run(self, args):
        """ Runs the command
        :param args: Arguments from argparse.Namespace
        """
        input_file = Path(args.input_file)
        output_file = Path(args.output_file)
        if args.mode == AdjustVertexOrderMode.INVERT:
            print("Reversing vertex order of polygons in spatial columns")
        else:
            print("Adjusting vertex order of polygons assuming data source with "
                  "flat - earth topology in spatial columns")

        # Starts the Hyper Process with telemetry enabled to send data to Tableau.
        # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

            with Connection(endpoint=hyper.endpoint) as connection:
                catalog = connection.catalog

                # Create output database file, fails if the file already exists
                catalog.create_database(output_file)

                # Attach input and output database files
                catalog.attach_database(input_file, "input")
                catalog.attach_database(output_file, "output")

                # Iterate over all schemas in the input file
                for schema_name in catalog.get_schema_names("input"):

                    # Create the schema in the output file
                    catalog.create_schema_if_not_exists(SchemaName("output", schema_name.name))

                    # Iterate over all tables in the input schema
                    for in_table in catalog.get_table_names(schema_name):
                        table_definition = catalog.get_table_definition(name=in_table)
                        columns = table_definition.columns

                        out_table = TableName("output", schema_name.name, in_table.name)
                        out_table_definition = TableDefinition(out_table, columns)

                        # Create the table in the output file with the same table definition.
                        # Note that any constraints on the table in the input file
                        # will not be present in the table in the output file
                        catalog.create_table(out_table_definition)

                        spatial_columns = [c.name for c in columns if c.type == SqlType.geography()]
                        in_column_modifiers = [f"geo_{args.mode.value}_vertex_order({c.name})"
                                               if c.type == SqlType.geography() else f"{c.name}" for c in columns]

                        if len(spatial_columns) > 0:
                            print(f"Copying table {in_table} with {len(spatial_columns)} "
                                  f"spatial columns: {spatial_columns}...")
                        else:
                            print(f"Copying table {in_table} with no spatial columns...")

                        row_count = connection.execute_command(
                            f"INSERT INTO {out_table} SELECT {','.join(in_column_modifiers)} FROM {in_table}")
                        print(f"   {row_count} rows copied")


def main(argv):
    command_map = {}
    command_map['list'] = ListTables()
    command_map['run'] = AdjustVertexOrder()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", help="Available commands", dest="command")
    subparsers.required = True
    for name, command in command_map.items():
        cmd_parser = subparsers.add_parser(name, help=command.Description)
        command.define_args(cmd_parser)

    args = parser.parse_args(argv)
    command = command_map[args.command]
    command.run(args)


if __name__ == "__main__":
    main(sys.argv[1:])
