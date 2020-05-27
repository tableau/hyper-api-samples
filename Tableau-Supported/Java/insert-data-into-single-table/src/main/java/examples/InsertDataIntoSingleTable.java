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
import java.util.List;

import static com.tableau.hyperapi.Nullability.NOT_NULLABLE;

/**
 * An example demonstrating a simple single-table Hyper file including table creation and data insertion with different types
 */
public class InsertDataIntoSingleTable {
    /**
     * The table is called "Extract" and will be created in the "Extract" schema.
     * This has historically been the default table name and schema for extracts created by Tableau
     */
    private static TableDefinition EXTRACT_TABLE = new TableDefinition(
            new TableName("Extract", "Extract"))
            .addColumn("Customer ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Customer Name", SqlType.text(), NOT_NULLABLE)
            .addColumn("Loyalty Reward Points", SqlType.bigInt(), NOT_NULLABLE)
            .addColumn("Segment", SqlType.text(), NOT_NULLABLE);

    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Insert data into a single table into a new Hyper file");

        Path customerDatabasePath = Paths.get("customers.hyper");

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {
            // Creates new Hyper file "customer.hyper"
            // Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists
            try (Connection connection = new Connection(process.getEndpoint(),
                    customerDatabasePath.toString(),
                    CreateMode.CREATE_AND_REPLACE)) {
                Catalog catalog = connection.getCatalog();

                catalog.createSchema(new SchemaName("Extract"));
                catalog.createTable(EXTRACT_TABLE);

                // Insert data into "Extract"."Extract" table
                try (Inserter inserter = new Inserter(connection, EXTRACT_TABLE)) {
                    inserter.add("DK-13375").add("Dennis Kane").add(518).add("Consumer").endRow();
                    inserter.add("EB-13705").add("Ed Braxton").add(815).add("Corporate").endRow();
                    inserter.execute();
                }

                // The table names in the "Extract" schema
                List<TableName> tablesInDatabase = catalog.getTableNames(new SchemaName("Extract"));
                System.out.println("Tables available in " + customerDatabasePath + " are: " + tablesInDatabase);

                // Number of rows in the "Extract"."Extract" table
                // executeScalarQuery is for executing a query that returns exactly one row with one column
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
