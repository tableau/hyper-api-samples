using System;

using Tableau.HyperAPI;

namespace Example
{
    internal class InsertDataIntoMultipleTables : Example
    {
        /// <summary>
        /// Create a new Hyper file with multiple tables and write some data into them.
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Insert data into multiple tables within a new Hyper file.");

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Connect to Hyper and create new Hyper file "superstore.hyper".
                // It replaces the file if it already exists when CreateMode.CreateAndReplace is set.
                using (Connection connection = new Connection(hyper.Endpoint, "superstore.hyper", CreateMode.CreateAndReplace))
                {
                    // Create definitions for the tables to be created.

                    // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                    TableDefinition orders = new TableDefinition("Orders")
                        .AddColumn("Address ID", SqlType.SmallInt(), Nullability.NotNullable)
                        .AddColumn("Customer ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Order Date", SqlType.Date(), Nullability.NotNullable)
                        .AddColumn("Order ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Ship Date", SqlType.Date())
                        .AddColumn("Ship Mode", SqlType.Text());

                    // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                    TableDefinition customer = new TableDefinition("Customer")
                        .AddColumn("Customer ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Customer Name", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Loyalty Reward Points", SqlType.BigInt(), Nullability.NotNullable)
                        .AddColumn("Segment", SqlType.Text(), Nullability.NotNullable);

                    // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                    TableDefinition products = new TableDefinition("Products")
                        .AddColumn("Category", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Product ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Product Name", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Sub-Category", SqlType.Text(), Nullability.NotNullable);

                    // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                    TableDefinition lineItems = new TableDefinition("Line Items")
                        .AddColumn("Line Item ID", SqlType.BigInt(), Nullability.NotNullable)
                        .AddColumn("Order ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Product ID", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Sales", SqlType.Double(), Nullability.NotNullable)
                        .AddColumn("Quantity", SqlType.SmallInt(), Nullability.NotNullable)
                        .AddColumn("Discount", SqlType.Double())
                        .AddColumn("Profit", SqlType.Double(), Nullability.NotNullable);

                    // Create tables in the database.
                    connection.Catalog.CreateTable(orders);
                    connection.Catalog.CreateTable(customer);
                    connection.Catalog.CreateTable(products);
                    connection.Catalog.CreateTable(lineItems);

                    // Insert data into Orders table.
                    using (Inserter inserter = new Inserter(connection, orders))
                    {
                        inserter.AddRow(399, "DK-13375", new Date(2012, 9, 7), "CA-2011-100006", new Date(2012, 9, 13), "Standard Class");
                        inserter.AddRow(530, "EB-13705", new Date(2012, 7, 8), "CA-2011-100090", new Date(2012, 7, 12), "Standard Class");
                        inserter.Execute();
                    }

                    // Insert data into Customers table.
                    using (Inserter inserter = new Inserter(connection, customer))
                    {
                        inserter.AddRow("DK-13375", "Dennis Kane", 518, "Consumer");
                        inserter.AddRow("EB-13705", "Ed Braxton", 815, "Corporate");
                        inserter.Execute();
                    }

                    // Insert data into Product table.
                    using (Inserter inserter = new Inserter(connection, products))
                    {
                        inserter.AddRow("TEC-PH-10002075", "Technology", "Phones", "AT&T EL51110 DECT");
                        inserter.Execute();
                    }

                    // Insert data into Line Items table.
                    using (Inserter inserter = new Inserter(connection, lineItems))
                    {
                        inserter.AddRow(2718, "CA-2011-100006", "TEC-PH-10002075", 377.97, 3, 0.0, 109.6113);
                        inserter.AddRow(2719, "CA-2011-100090", "TEC-PH-10002075", 377.97, 3, null, 109.6113);
                        inserter.Execute();
                    }

                    foreach (var name in new[] { orders.TableName, customer.TableName, products.TableName, lineItems.TableName })
                    {
                        // ExecuteScalarQuery is for executing a query that returns exactly one row with one column
                        long count = connection.ExecuteScalarQuery<long>($"SELECT COUNT(*) FROM {name}");
                        Console.WriteLine($"Table {name} has a count of {count} rows");
                    }
                }

                Console.WriteLine("The connection to the Hyper file has been closed.");
            }

            Console.WriteLine("The Hyper process has been shut down.");
        }
    }
}
