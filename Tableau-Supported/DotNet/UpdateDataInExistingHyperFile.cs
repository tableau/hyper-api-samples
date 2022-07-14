using System;
using System.IO;

using Tableau.HyperAPI;

namespace Example
{
    internal class UpdateDataInExistingHyperFile : Example
    {
        /// <summary>
        /// Open a Hyper file and modify some data in it.
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Update data in an existing Hyper file.");

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Make a copy of the Hyper file to modify.
                string sourceDatabase = Path.Join(exampleDataDir, "superstore_sample.hyper");
                string database = "superstore_sample_update.hyper";
                File.Copy(sourceDatabase, database, true);

                // Connect to the Hyper file.
                using (Connection connection = new Connection(hyper.Endpoint, database))
                {
                    Console.WriteLine("Pre-Update: Individual rows showing 'Segment' and 'Loyalty Reward Points':");
                    using (Result result = connection.ExecuteQuery(
                        $"SELECT {Sql.EscapeName("Loyalty Reward Points")}, {Sql.EscapeName("Segment")}" +
                        $"FROM {Sql.EscapeName("Customer")}"))
                    {
                        while (result.NextRow())
                        {
                            Console.WriteLine($"[{string.Join(", ", result.GetValues())}]");
                        }
                    }

                    Console.WriteLine("Update 'Customers' table by adding 50 Loyalty Reward Points to all Corporate Customers");
                    int updatedRowCount = connection.ExecuteCommand(
                        $"UPDATE {Sql.EscapeName("Customer")} " +
                        $"SET {Sql.EscapeName("Loyalty Reward Points")} = {Sql.EscapeName("Loyalty Reward Points")} + 50 " +
                        $"WHERE {Sql.EscapeName("Segment")} = {Sql.EscapeStringLiteral("Corporate")}");

                    Console.WriteLine($"The number of updated rows in 'Customer' table is {updatedRowCount}");

                    Console.WriteLine("Post-Update: Individual rows showing 'Segment' and 'Loyalty Reward Points':");
                    using (Result result = connection.ExecuteQuery(
                        $"SELECT {Sql.EscapeName("Loyalty Reward Points")}, {Sql.EscapeName("Segment")}" +
                        $"FROM {Sql.EscapeName("Customer")}"))
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
