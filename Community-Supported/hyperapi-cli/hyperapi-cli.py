#!/usr/bin/env python3
import readline
from argparse import ArgumentParser
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, HyperException


def main():
    parser = ArgumentParser("HyperAPI interactive cli.")
    parser.add_argument("database", type=str, nargs='?',
                        help="A Hyper file to attach on startup")

    args = parser.parse_args()
    create_mode = CreateMode.CREATE_IF_NOT_EXISTS if args.database else CreateMode.NONE

    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper_process:
        try:
            with Connection(hyper_process.endpoint, args.database, create_mode) as connection:
                while True:
                    try:
                        sql = input("> ")
                    except (EOFError, KeyboardInterrupt):
                        return
                    try:
                        with connection.execute_query(sql) as result:
                            print("\t".join(str(column.name)
                                  for column in result.schema.columns))
                            for row in result:
                                print("\t".join(str(column) for column in row))
                    except HyperException as exception:
                        print(f"Error executing SQL: {exception}")
        except HyperException as exception:
            print(f"Unable to connect to the database: {exception}")


if __name__ == "__main__":
    main()
