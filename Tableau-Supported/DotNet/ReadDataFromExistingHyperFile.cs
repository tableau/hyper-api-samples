using System;
using System.IO;

using Tableau.HyperAPI;

namespace Example
{
    internal class ReadDataFromExistingHyperFile : Example
    {
        /// <summary>
        /// Open a Hyper file and read data from it.
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Read data from an existing Hyper file.");

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Connect to the Hyper file.
                using (Connection connection = new Connection(hyper.Endpoint, Path.Join(exampleDataDir, "superstore_sample_denormalized.hyper")))
                {
                    // Get all tables in the "Extract" schema of the Hyper file
                    foreach (TableName table in connection.Catalog.GetTableNames("Extract"))
                    {
                        TableDefinition tableDef = connection.Catalog.GetTableDefinition(table);
                        Console.WriteLine($"Table {table.Name} has qualified name: {tableDef.TableName}");
                        // Get all the columns in the table.
                        foreach (TableDefinition.Column column in tableDef.Columns)
                        {
                            Console.WriteLine($"Column {column.Name} has type={column.Type} and nullabilty={column.Nullability}");
                        }
                    }

                    // Print all rows from the "Extract"."Extract" table.
                    TableName tableName = new TableName("Extract", "Extract");
                    Console.WriteLine($"These are all rows in the table {tableName}");
                    using (Result result = connection.ExecuteQuery($"SELECT * FROM {tableName}"))
                    {
                        while (result.NextRow())
                        {
                            Console.WriteLine($"[{string.Join(", ", result.GetValues())}]");
                        }
                    }
                }

                Console.WriteLine("The connection to the Hyper file has been closed.");
            }

            Console.WriteLine("The Hyper process has been shut down.");
        }
    }
}
