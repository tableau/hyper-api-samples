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
 * An example demonstrating pushing down computations to Hyper during Insertion
 */
public class InsertDataWithExpressions {
    /**
     * The table is called "Extract" and will be created in the "Extract" schema.
     * This has historically been the default table name and schema for extracts created by Tableau
     */
    private static TableDefinition EXTRACT_TABLE = new TableDefinition(
            new TableName("Extract","Extract"))
                .addColumn("Order ID", SqlType.integer(), NOT_NULLABLE)
                .addColumn("Ship Timestamp", SqlType.timestamp(), NOT_NULLABLE)
                .addColumn("Ship Mode", SqlType.text(), NOT_NULLABLE)
                .addColumn("Ship Priority", SqlType.integer(), NOT_NULLABLE);

    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Push down computations to Hyper during insertion with expressions");

        Path ordersPath = Paths.get(getWorkingDirectory(), "orders.hyper");

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {
            // Creates new Hyper file "orders.hyper"
            // Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists
            try (Connection connection = new Connection(process.getEndpoint(),
                    ordersPath.toString(),
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
                //       For example new Inserter.ColumnMapping("no_data_transformation_column")
                //   4. Inserter Definition, a list of column definitions for all the input values provided during insertion.

                // Inserter definition contains the column definition for the values that are inserted.
                List<TableDefinition.Column> inserterDefinition = new ArrayList<TableDefinition.Column>();
                inserterDefinition.add(new TableDefinition.Column("Order ID", SqlType.integer(), NOT_NULLABLE));
                inserterDefinition.add(new TableDefinition.Column("Ship Timestamp Text", SqlType.text(), NOT_NULLABLE));
                inserterDefinition.add(new TableDefinition.Column("Ship Mode", SqlType.text(), NOT_NULLABLE));
                inserterDefinition.add(new TableDefinition.Column("Ship Priority Text", SqlType.text(), NOT_NULLABLE));

                // Column 'Order Id' is inserted into "Extract"."Extract" as-is.
                // Column 'Ship Timestamp' in "Extract"."Extract" of timestamp type is computed from Column 'Ship Timestamp Text' of text type using 'to_timestamp()'.
                // Column 'Ship Mode' is inserted into "Extract"."Extract" as-is.
                // Column 'Ship Priority' is "Extract"."Extract" of integer type is computed from Colum 'Ship Priority Text' of text type using 'CASE' statement.
                String textToTimeStampExpression = "to_timestamp(" + escapeName("Ship Timestamp Text") + ", "+ escapeStringLiteral("YYYY-MM-DD HH24:MI:SS") + ")";
                String shipPriorityAsIntCaseExpression = "CASE " + escapeName("Ship Priority Text") +
                                                         " WHEN " + escapeStringLiteral("Urgent") + " THEN 1 " +
                                                         " WHEN " + escapeStringLiteral("Medium") + " THEN 2 " +
                                                         " WHEN " + escapeStringLiteral("Low") + " THEN 3 END";

                List<Inserter.ColumnMapping> columnMappings = new ArrayList<Inserter.ColumnMapping>();
                columnMappings.add(new Inserter.ColumnMapping("Order ID"));
                columnMappings.add(new Inserter.ColumnMapping("Ship Timestamp", textToTimeStampExpression));
                columnMappings.add(new Inserter.ColumnMapping("Ship Mode"));
                columnMappings.add(new Inserter.ColumnMapping("Ship Priority", shipPriorityAsIntCaseExpression));

               // Insert data into "Extract"."Extract" table with expressions.
                try (Inserter inserter = new Inserter(connection, EXTRACT_TABLE, columnMappings, inserterDefinition)) {
                    inserter.add(399).add("2012-09-13 10:00:00").add("Express Class").add("Urgent").endRow();
                    inserter.add(530).add("2012-07-12 14:00:00").add("Standard Class").add("Low").endRow();
                    inserter.execute();
                }

                // The table names in the "Extract" schema.
                List<TableName> tablesInDatabase = catalog.getTableNames(new SchemaName("Extract"));
                System.out.println("Tables available in " + ordersPath + " are: " + tablesInDatabase );

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

    /**
     * Returns the current working directory
     *
     * @return The inferred working directory
     */
    private static String getWorkingDirectory() {
        return System.getProperty("user.dir");
    }
}
