using System;
using System.Collections.Generic;

using Tableau.HyperAPI;

namespace Example
{
    internal class InsertDataWithExpressions : Example
    {
        /// <summary>
        /// Push down computations to Hyper during Insertion using expressions
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Push down computations to Hyper during Insertion using Expressions");

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Connect to hyper and create new Hyper file "superstore.hyper".
                // Replaces file if it already exists when CreateMode.CreateAndReplace is set.
                using (Connection connection = new Connection(hyper.Endpoint, "orders.hyper", CreateMode.CreateAndReplace))
                {
                    // The table is called "Extract" and will be created in the "Extract" schema.
                    // This has historically been the default table name and schema for extracts created by Tableau.
                    TableName extractTable = new TableName("Extract", "Extract");
                    TableDefinition extractTableDefinition = new TableDefinition(extractTable)
                        .AddColumn("Order ID", SqlType.Int(), Nullability.NotNullable)
                        .AddColumn("Ship Timestamp", SqlType.Timestamp(), Nullability.NotNullable)
                        .AddColumn("Ship Mode", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Ship Priority", SqlType.Int(), Nullability.NotNullable);

                    // Create the schema and the table
                    connection.Catalog.CreateSchema("Extract");
                    connection.Catalog.CreateTable(extractTableDefinition);

                    // Hyper API's Inserter allows users to transform data during insertion.
                    // To make use of data transformation during insertion, the inserter requires the following inputs
                    //   1. The connection to the Hyper instance containing the table.
                    //   2. The table name or table defintion into which data is inserted.
                    //   3. List of Inserter.ColumnMapping.
                    //       This list informs the inserter how each column in the target table must be tranformed.
                    //       The list must contain all the columns into which data is inserted.
                    //       "Inserter.ColumnMapping" maps a valid SQL expression (if any) to a column in the target table
                    //       For example new Inserter.ColumnMapping("target_column", $"{Sql.EscapeName("colA")}*{Sql.EscapeName("colB")})
                    //       The column "target_column" contains the product of "colA" and "colB" after successful insertion.
                    //       SQL expression string is optional in Inserter.ColumnMapping.
                    //       For a column without any transformation (identity transformation) only the column name is required.
                    //       For example new Inserter.ColumnMapping("no_data_transformation_column")
                    //   4. Inserter Definition, a list of column definitions for all the input values provided during insertion.

                    // Inserter definition contains the column definition for the values that are inserted.
                    List<TableDefinition.Column> inserterDefinition = new List<TableDefinition.Column>();
                    inserterDefinition.Add(new TableDefinition.Column("Order ID", SqlType.Int(), Nullability.NotNullable));
                    inserterDefinition.Add(new TableDefinition.Column("Ship Timestamp Text", SqlType.Text(), Nullability.NotNullable));
                    inserterDefinition.Add(new TableDefinition.Column("Ship Mode", SqlType.Text(), Nullability.NotNullable));
                    inserterDefinition.Add(new TableDefinition.Column("Ship Priority Text", SqlType.Text(), Nullability.NotNullable));

                    // Column 'Order Id' is inserted into "Extract"."Extract" as-is.
                    // Column 'Ship Timestamp' in "Extract"."Extract" of timestamp type is computed from Column 'Ship Timestamp Text' of text type using 'to_timestamp()'.
                    // Column 'Ship Mode' is inserted into "Extract"."Extract" as-is.
                    // Column 'Ship Priority' is "Extract"."Extract" of integer type is computed from Colum 'Ship Priority Text' of text type using 'CASE' statement.
                    string textToTimeStampExpression = $"to_timestamp({Sql.EscapeName("Ship Timestamp Text")}, {Sql.EscapeStringLiteral("YYYY-MM-DD HH24:MI:SS")})";
                    string shipPriorityAsIntCaseExpression = $"CASE {Sql.EscapeName("Ship Priority Text")}" +
                                                             $" WHEN {Sql.EscapeStringLiteral("Urgent")} THEN 1 " +
                                                             $" WHEN {Sql.EscapeStringLiteral("Medium")} THEN 2 " +
                                                             $" WHEN {Sql.EscapeStringLiteral("Low")} THEN 3 END";

                    List<Inserter.ColumnMapping> columnMappings = new List<Inserter.ColumnMapping>();
                    columnMappings.Add(new Inserter.ColumnMapping("Order ID"));
                    columnMappings.Add(new Inserter.ColumnMapping("Ship Timestamp", textToTimeStampExpression));
                    columnMappings.Add(new Inserter.ColumnMapping("Ship Mode"));
                    columnMappings.Add(new Inserter.ColumnMapping("Ship Priority", shipPriorityAsIntCaseExpression));

                    // Insert data into the "Extract"."Extract" table with expressions.
                    using (Inserter inserter = new Inserter(connection, extractTable, columnMappings, inserterDefinition))
                    {
                        inserter.AddRow(399, "2012-09-13 10:00:00", "Express Class", "Urgent");
                        inserter.AddRow(530, "2012-07-12 14:00:00", "Standard Class", "Low");
                        inserter.Execute();
                    }

                    // ExecuteScalarQuery is for executing a query that returns exactly one row with one column.
                    long count = connection.ExecuteScalarQuery<long>($"SELECT COUNT(*) FROM {extractTable}");
                    Console.WriteLine($"Table {extractTable} has a count of {count} rows");
                }

                Console.WriteLine("The connection to the Hyper file has been closed.");
            }

            Console.WriteLine("The Hyper process has been shut down.");
        }
    }
}
