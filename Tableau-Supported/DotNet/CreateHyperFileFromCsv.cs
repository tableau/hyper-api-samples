using System;
using System.IO;
using System.Collections.Generic;

using Tableau.HyperAPI;

namespace Example
{
    internal class CreateHyperFileFromCsv : Example
    {
        /// <summary>
        /// Create a new Hyper file with a single table and load data from a CSV file into it.
        /// For more details, see https://tableau.github.io/hyper-db/docs/guides/hyper_file/insert_csv
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Load data from a CSV file into a table in a new Hyper file.");

            // Optional process parameters. They are documented in the Tableau Hyper documentation, chapter "Process Settings"
            // (https://tableau.github.io/hyper-db/docs/hyper-api/hyper_process#process-settings).
            var processParameters = new Dictionary<string, string>
            {
                // Limits the number of Hyper event log files to two.
                { "log_file_max_count", "2" },
                // Limits the size of Hyper event log files to 100 megabytes.
                { "log_file_size_limit", "100M" }
            };

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau, "example", processParameters))
            {
                // Optional connection parameters. They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
                // (https://tableau.github.io/hyper-db/docs/hyper-api/connection#connection-settings).
                var connectionParameters = new Dictionary<string, string>
                {
                    { "lc_time", "en_US" }
                };

                // Connect to Hyper and create new Hyper file "customer.hyper".
                // It replaces the file if it already exists when CreateMode.CreateAndReplace is set.
                using (Connection connection = new Connection(hyper.Endpoint, "customer.hyper", CreateMode.CreateAndReplace, connectionParameters))
                {
                    // Table definition - its name and the list of columns.
                    // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                    TableDefinition customerTable = new TableDefinition("Customer")
                       .AddColumn("Customer ID", SqlType.Text(), Nullability.NotNullable)
                       .AddColumn("Customer Name", SqlType.Text(), Nullability.NotNullable)
                       .AddColumn("Loyalty Reward Points", SqlType.BigInt(), Nullability.NotNullable)
                       .AddColumn("Segment", SqlType.Text(), Nullability.NotNullable);

                    // Create the table in the Hyper file.
                    connection.Catalog.CreateTable(customerTable);

                    string pathToCsv = Path.Join(exampleDataDir, "customers.csv");

                    // Load all rows into "Customers" table from the CSV file.
                    // ExecuteCommand executes a SQL statement and returns the impacted row count.
                    // TableDefinition.Name property is a QualifiedName object which is escaped properly when
                    // converted to a string; but the path to the CSV file needs to be escaped.
                    //
                    // Note:
                    // You might have to adjust the COPY parameters to the format of your specific csv file.
                    // The example assumes that your columns are separated with the ',' character
                    // and that NULL values are encoded via the string 'NULL'.
                    // Also be aware that the `header` option is used in this example:
                    // It treats the first line of the csv file as a header and does not import it.
                    //
                    // The parameters of the COPY command are documented in the Tableau Hyper SQL documentation
                    // (https://tableau.github.io/hyper-db/docs/sql/command/copy_from).
                    Console.WriteLine("Issuing the SQL COPY command to load the csv file into the table. Since the first line");
                    Console.WriteLine("of our csv file contains the column names, we use the `header` option to skip it.");
                    int countInCustomerTable = connection.ExecuteCommand(
                        $"COPY {customerTable.TableName} from {Sql.EscapeStringLiteral(pathToCsv)} with " +
                        $"(format csv, NULL 'NULL', delimiter ',', header)");

                    Console.WriteLine($"The number of rows in table {customerTable.TableName} is {countInCustomerTable}");
                }

                Console.WriteLine("The connection to the Hyper file has been closed.");
            }

            Console.WriteLine("The Hyper process has been shut down.");
        }
    }
}
