using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

using Tableau.HyperAPI;

namespace Example
{
    internal class DeleteDataInExistingHyperFile : Example
    {
        /// <summary>
        /// Open a Hyper file and delete some data from it.
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string dataDir)
        {
            Console.WriteLine("EXAMPLE - Delete data from an existing Hyper file.");

            // Hyper file containing data in Customer, Product, Orders and LineItems tables.
            string sourceDatabase = Path.Join(dataDir, "superstore_sample.hyper");

            // Make a copy of the Hyper file to modify.
            string database = "superstore_sample_delete.hyper";
            File.Copy(sourceDatabase, database, true);

            // Start Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Connect to the Hyper file.
                using (Connection connection = new Connection(hyper.Endpoint, database))
                {
                    Console.WriteLine("Delete all rows from customer with the name 'Dennis Kane' from table Orders");
                    // ExecuteCommand executes a SQL statement and returns the impacted row count
                    int deletedRowCount = connection.ExecuteCommand(
                        $"DELETE FROM {Sql.EscapeName("Orders")} " +
                        $"WHERE {Sql.EscapeName("Customer ID")} = ANY(" +
                        $"SELECT {Sql.EscapeName("Customer ID")} FROM {Sql.EscapeName("Customer")} " +
                        $"WHERE {Sql.EscapeName("Customer Name")} = {Sql.EscapeStringLiteral("Dennis Kane")})");
                    Console.WriteLine($"The number of deleted rows in table Orders is {deletedRowCount}\n");

                    Console.WriteLine("Delete all rows from customer with the name 'Dennis Kane' from table Customer");
                    deletedRowCount = connection.ExecuteCommand(
                        $"DELETE FROM {Sql.EscapeName("Customer")} " +
                        $"WHERE {Sql.EscapeName("Customer Name")} = {Sql.EscapeStringLiteral("Dennis Kane")}");
                    Console.WriteLine($"The number of deleted rows in table Customer is {deletedRowCount}\n");
                }

                Console.WriteLine("The connection to the Hyper file has been closed.");
            }

            Console.WriteLine("The Hyper process has been shut down.");
        }
    }
}
