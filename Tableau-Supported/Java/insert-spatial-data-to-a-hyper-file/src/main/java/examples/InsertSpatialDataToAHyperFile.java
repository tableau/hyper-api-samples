//---------------------------------------------------------------------------
//
// This file is the copyrighted property of Tableau Software and is protected
// by registered patents and other applicable U.S. and international laws and
// regulations.
//
// You may adapt this file and modify it to fit into your context and use it
// as a template to start your own projects.
//
//---------------------------------------------------------------------------
package examples;

import com.tableau.hyperapi.*;

import javax.xml.validation.Schema;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.tableau.hyperapi.Nullability.NOT_NULLABLE;
import static com.tableau.hyperapi.Sql.escapeName;
import static com.tableau.hyperapi.Sql.escapeStringLiteral;

/**
 * An example demonstrating spatial data insertion to a hyper file
 */
public class InsertSpatialDataToAHyperFile{
    /**
     * The table is called "Extract" and will be created in the "Extract" schema.
     * This has historically been the default table name and schema for extracts created by Tableau
     */
    private static TableDefinition EXTRACT_TABLE = new TableDefinition(
            new TableName("Extract","Extract"))
            .addColumn("Name", SqlType.text(), NOT_NULLABLE)
            .addColumn("Location", SqlType.geography(), NOT_NULLABLE);

    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Insert spatial data into a Hyper file");

        Path spatialDataPath = Paths.get("spatial_data.hyper");

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {
            // Creates new Hyper file "spatial_data.hyper"
            // Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists
            try (Connection connection = new Connection(process.getEndpoint(),
                    spatialDataPath.toString(),
                    CreateMode.CREATE_AND_REPLACE)) {
                Catalog catalog = connection.getCatalog();

                catalog.createSchema(new SchemaName("Extract"));
                catalog.createTable(EXTRACT_TABLE);

                // Hyper API's Inserter allows users to transform data during insertion.
                // To make use of data transformation during insertion, the inserter requires the following inputs
                //   1. The connection to the Hyper instance containing the table.
                //   2. The table name or table defintion into which data is inserted.
                //   3. List of Inserter.ColumnMapping.
                //       This list informs the inserter how each column in the target table must be tranformed.
                //       The list must contain all the columns into which data is inserted.
                //       "Inserter.ColumnMapping" maps a valid SQL expression (if any) to a column in the target table.
                //       For example new Inserter.ColumnMapping("target_column", 'escapeName("colA") + "*" + escapeName("colB")')
                //       The column "target_column" contains the product of "colA" and "colB" after successful insertion.
                //       SQL expression string is optional in Inserter.ColumnMapping.
                //       For a column without any transformation (identity transformation) only the column name is required.
                //       For example new Inserter.ColumnMapping('no_data_transformation_column')
                //   4. Inserter Definition, a list of column definitions for all the input values provided during insertion.

                // The inserter definition contains the column definition for the values that are inserted.
                // The data input has two text values Name and Location_as_text.
                List<TableDefinition.Column> inserterDefintion = new ArrayList<TableDefinition.Column>();
                inserterDefintion.add(new TableDefinition.Column("Name", SqlType.text(), NOT_NULLABLE));
                inserterDefintion.add(new TableDefinition.Column("Location_as_text", SqlType.text(), NOT_NULLABLE));

                // Column 'Name' is inserted into "Extract"."Extract" as-is.
                // Column 'Location' in "Extract"."Extract" of geography type is computed from Column 'Location_as_text' of text type
                // using the expression 'CAST("Location_as_text") AS GEOGRAPHY'.
                // Inserter.ColumnMapping is used for mapping the CAST expression to Column 'Location'.
                String textToGeographyCastExpression = "CAST(" + escapeName("Location_as_text") + " AS GEOGRAPHY)";
                List<Inserter.ColumnMapping> columnMappings = new ArrayList<Inserter.ColumnMapping>();
                columnMappings.add(new Inserter.ColumnMapping("Name"));
                columnMappings.add(new Inserter.ColumnMapping("Location", textToGeographyCastExpression));

               // Insert data into "Extract"."Extract" table with CAST expression.
                try(Inserter inserter = new Inserter(connection, EXTRACT_TABLE, columnMappings, inserterDefintion)){
                    inserter.add("Seattle").add("point(-122.338083 47.647528)").endRow();
                    inserter.add("Munich").add("point(11.584329 48.139257)").endRow();
                    inserter.execute();
                }

                // Number of rows in the "Extract"."Extract" table.
                // executeScalarQuery is for executing a query that returns exactly one row with one column.
                long rowCount = connection.<Long>executeScalarQuery(
                        "SELECT COUNT(*) FROM " + EXTRACT_TABLE.getTableName()
                ).get();
                System.out.println("The number of rows in table " + EXTRACT_TABLE.getTableName() + " is " + rowCount + "\n");
            }
            System.out.println("The connection to the Hyper file has been closed");
        }
        System.out.println("The Hyper process has been shut down");
    }
}
