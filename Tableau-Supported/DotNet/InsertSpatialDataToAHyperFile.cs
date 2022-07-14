using System;
using System.Collections.Generic;

using Tableau.HyperAPI;

namespace Example
{
    internal class InsertSpatialDataToAHyperFile : Example
    {
        /// <summary>
        /// Create a new Hyper file with a single table and write spatial data into it.
        /// </summary>
        /// <param name="exampleDataDir">Path to the directory with example data.</param>
        public override void Execute(string exampleDataDir)
        {
            Console.WriteLine("EXAMPLE - Insert spatial data into a single tables within a new Hyper file.");

            // Start the Hyper process with telemetry enabled.
            using (HyperProcess hyper = new HyperProcess(Telemetry.SendUsageDataToTableau))
            {
                // Connect to hyper and create new Hyper file "spatial_data.hyper".
                // Replaces file if it already exists when CreateMode.CreateAndReplace is set.
                using (Connection connection = new Connection(hyper.Endpoint, "spatial_data.hyper", CreateMode.CreateAndReplace))
                {
                    // The table is called "Extract" and will be created in the "Extract" schema.
                    // This has historically been the default table name and schema for extracts created by Tableau.
                    TableName extractTable = new TableName("Extract", "Extract");
                    TableDefinition extractTableDefinition = new TableDefinition(extractTable)
                        .AddColumn("Name", SqlType.Text(), Nullability.NotNullable)
                        .AddColumn("Location", SqlType.Geography(), Nullability.NotNullable);

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
                    // The data input has two text values Name and Location_as_text.
                    List<TableDefinition.Column> inserterDefinition = new List<TableDefinition.Column>();
                    inserterDefinition.Add(new TableDefinition.Column("Name", SqlType.Text(), Nullability.NotNullable));
                    inserterDefinition.Add(new TableDefinition.Column("Location_as_text", SqlType.Text(), Nullability.NotNullable));

                    // Column 'Name' is inserted into "Extract"."Extract" as-is.
                    // Column 'Location' in "Extract"."Extract" of geography type is computed from Column 'Location_as_text' of text type
                    // using the expression 'CAST("Location_as_text") AS GEOGRAPHY'.
                    // Inserter.ColumnMapping is used for mapping the CAST expression to Column 'Location'.
                    string textToGeographyCastExpression = $"CAST({Sql.EscapeName("Location_as_text")} AS GEOGRAPHY)";
                    List<Inserter.ColumnMapping> columnMappings = new List<Inserter.ColumnMapping>();
                    columnMappings.Add(new Inserter.ColumnMapping("Name"));
                    columnMappings.Add(new Inserter.ColumnMapping("Location", textToGeographyCastExpression));

                    // Insert spatial data into the "Extract"."Extract" table using CAST expression.
                    using (Inserter inserter = new Inserter(connection, extractTableDefinition, columnMappings, inserterDefinition))
                    {
                        inserter.AddRow("Seattle", "point(-122.338083 47.647528)");
                        inserter.AddRow("Munich" , "point(11.584329 48.139257)");
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
