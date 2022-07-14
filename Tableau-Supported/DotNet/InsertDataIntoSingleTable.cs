using System;

using Tableau.HyperAPI;

namespace Example
{
    internal class InsertDataIntoSingleTable : Example
    {
        /// <summary>
        /// Create a new Hyper file with a single table and write some data into it.
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Insert data into a single tables within a new Hyper file.");

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Connect to hyper and create new Hyper file "superstore.hyper".
                // Replaces file if it already exists when CreateMode.CreateAndReplace is set.
                using (Connection connection = new Connection(hyper.Endpoint, "superstore.hyper", CreateMode.CreateAndReplace))
                {
                    // The table is called "Extract" and will be created in the "Extract" schema.
                    // This has historically been the default table name and schema for extracts created by Tableau.
                    TableName extractTable = new TableName("Extract", "Extract");
                    TableDefinition extractTableDefinition = new TableDefinition(extractTable)
                        .AddColumn("Customer ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Customer Name", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Loyalty Reward Points", SqlType.BigInt(), Nullability.NotNullable)
                        .AddColumn("Segment", SqlType.Text(), Nullability.NotNullable);

                    // Create the schema and the table
                    connection.Catalog.CreateSchema("Extract");
                    connection.Catalog.CreateTable(extractTableDefinition);

                    // Insert data into the "Extract"."Extract" table
                    using (Inserter inserter = new Inserter(connection, extractTable))
                    {
                        inserter.AddRow("DK-13375", "Dennis Kane", 518, "Consumer");
                        inserter.AddRow("EB-13705", "Ed Braxton", 815, "Corporate");
                        inserter.Execute();
                    }

                    // ExecuteScalarQuery is for executing a query that returns exactly one row with one column
                    long count = connection.ExecuteScalarQuery<long>($"SELECT COUNT(*) FROM {extractTable}");
                    Console.WriteLine($"Table {extractTable} has a count of {count} rows");
                }

                Console.WriteLine("The connection to the Hyper file has been closed.");
            }

            Console.WriteLine("The Hyper process has been shut down.");
        }
    }
}
