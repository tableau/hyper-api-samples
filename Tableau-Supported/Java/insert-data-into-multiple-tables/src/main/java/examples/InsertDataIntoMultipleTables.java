package examples;

import com.tableau.hyperapi.Catalog;
import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.CreateMode;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Inserter;
import com.tableau.hyperapi.TableName;
import com.tableau.hyperapi.SqlType;
import com.tableau.hyperapi.TableDefinition;
import com.tableau.hyperapi.Telemetry;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.tableau.hyperapi.Nullability.NOT_NULLABLE;
import static com.tableau.hyperapi.Nullability.NULLABLE;

/**
 * An example of how to create and insert data into a multi-table Hyper file where tables have different types
 */
public class InsertDataIntoMultipleTables {
    // Table Definitions required to create tables
    /**
     * The orders table
     */
    private static TableDefinition ORDERS_TABLE = new TableDefinition(
            // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
            new TableName("Orders"))
            .addColumn("Address ID", SqlType.smallInt(), NOT_NULLABLE)
            .addColumn("Customer ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Order Date", SqlType.date(), NOT_NULLABLE)
            .addColumn("Order ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Ship Date", SqlType.date(), NULLABLE)
            .addColumn("Ship Mode", SqlType.text(), NULLABLE);

    /**
     * The customer table
     */
    private static TableDefinition CUSTOMER_TABLE = new TableDefinition(
            // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
            new TableName("Customer"))
            .addColumn("Customer ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Customer Name", SqlType.text(), NOT_NULLABLE)
            .addColumn("Loyalty Reward Points", SqlType.bigInt(), NOT_NULLABLE)
            .addColumn("Segment", SqlType.text(), NOT_NULLABLE);

    /**
     * The products table
     */
    private static TableDefinition PRODUCTS_TABLE = new TableDefinition(
            // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
            new TableName("Products"))
            .addColumn("Category", SqlType.text(), NOT_NULLABLE)
            .addColumn("Product ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Product Name", SqlType.text(), NOT_NULLABLE)
            .addColumn("Sub-Category", SqlType.text(), NOT_NULLABLE);

    /**
     * The line items table
     */
    private static TableDefinition LINE_ITEMS_TABLE = new TableDefinition(
            // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
            new TableName("Line Items"))
            .addColumn("Line Item ID", SqlType.bigInt(), NOT_NULLABLE)
            .addColumn("Order ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Product ID", SqlType.text(), NOT_NULLABLE)
            .addColumn("Sales", SqlType.doublePrecision(), NOT_NULLABLE)
            .addColumn("Quantity", SqlType.smallInt(), NOT_NULLABLE)
            .addColumn("Discount", SqlType.doublePrecision(), NULLABLE)
            .addColumn("Profit", SqlType.doublePrecision(), NOT_NULLABLE);

    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Insert data into multiple tables within a new Hyper file\n");

        Path superstoreDatabasePath = Paths.get("customers.hyper");

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {
            // Creates new Hyper file "superstore.hyper"
            // Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists
            try (Connection connection = new Connection(process.getEndpoint(),
                    superstoreDatabasePath.toString(),
                    CreateMode.CREATE_AND_REPLACE)) {
                Catalog catalog = connection.getCatalog();
                catalog.createTable(ORDERS_TABLE);
                catalog.createTable(CUSTOMER_TABLE);
                catalog.createTable(PRODUCTS_TABLE);
                catalog.createTable(LINE_ITEMS_TABLE);

                // Insert data into Orders table
                try (Inserter inserter = new Inserter(connection, ORDERS_TABLE)) {
                    inserter.add((short) 399).add("DK-13375")
                            .addDate(2012, 9, 7).add("CA-2011-100006")
                            .addDate(2012, 9, 13).add("Standard Class").endRow();
                    inserter.add((short) 530).add("EB-13705")
                            .addDate(2012, 7, 8).add("CA-2011-100090")
                            .addDate(2012, 7, 12).add("Standard Class").endRow();
                    inserter.execute();
                }

                // Insert data into Customer table
                try (Inserter inserter = new Inserter(connection, CUSTOMER_TABLE)) {
                    inserter.add("DK-13375").add("Dennis Kane").add(518)
                            .add("Consumer").endRow();
                    inserter.add("EB-13705").add("Ed Braxton").add(815)
                            .add("Corporate").endRow();
                    inserter.execute();
                }

                // Insert data into Products table
                try (Inserter inserter = new Inserter(connection, PRODUCTS_TABLE)) {
                    inserter.add("TEC-PH-10002075").add("Technology").add("Phones")
                            .add("AT&T EL51110 DECT").endRow();
                    inserter.execute();
                }

                // Insert data into Line Items table
                try (Inserter inserter = new Inserter(connection, LINE_ITEMS_TABLE)) {
                    inserter.add(2718).add("CA-2011-100006").add("TEC-PH-10002075")
                            .add(377.97).add((short) 3).add(0)
                            .add(109.6113).endRow();
                    inserter.add(2719).add("CA-2011-100090").add("TEC-PH-10002075")
                            .add(377.97).add((short) 3).add(Double.NaN)
                            .add(109.6113).endRow();
                    inserter.execute();
                }

                TableDefinition[] tables = new TableDefinition[]{ORDERS_TABLE, CUSTOMER_TABLE, PRODUCTS_TABLE, LINE_ITEMS_TABLE};
                for (TableDefinition table : tables) {
                    // executeScalarQuery is for executing a query that returns exactly one row with one column
                    long countInTable = connection.<Long>executeScalarQuery("SELECT COUNT(*) FROM " + table.getTableName()).get();
                    System.out.println("The number of rows in table " + table.getTableName() + " is " + countInTable);
                }
                System.out.println();
            }
            System.out.println("The connection to the Hyper file has been closed");
        }
        System.out.println("The Hyper process has been shut down");
    }
}
