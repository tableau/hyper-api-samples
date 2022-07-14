package examples;

import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.CreateMode;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.TableName;
import com.tableau.hyperapi.TableDefinition;
import com.tableau.hyperapi.Telemetry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.tableau.hyperapi.Nullability.NOT_NULLABLE;
import static com.tableau.hyperapi.Sql.escapeStringLiteral;
import static com.tableau.hyperapi.SqlType.bigInt;
import static com.tableau.hyperapi.SqlType.text;
import static java.util.Arrays.asList;

/**
 * An example demonstrating loading data from a csv into a new Hyper file
 * For more details, see https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_insert_csv.html
 */
public class CreateHyperFileFromCSV {

    /**
     * The customer table
     */
    private static TableDefinition CUSTOMER_TABLE = new TableDefinition(
            // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
            new TableName("Customer"),
            asList(
                    new TableDefinition.Column("Customer ID", text(), NOT_NULLABLE),
                    new TableDefinition.Column("Customer Name", text(), NOT_NULLABLE),
                    new TableDefinition.Column("Loyalty Reward Points", bigInt(), NOT_NULLABLE),
                    new TableDefinition.Column("Segment", text(), NOT_NULLABLE)
            )
    );

    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE -  Load data from CSV into table in new Hyper file");

        Path customerDatabasePath = Paths.get("customers.hyper");

        // Optional process parameters. They are documented in the Tableau Hyper documentation, chapter "Process Settings"
        // (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
        Map<String, String> processParameters = new HashMap<>();
        // Limits the number of Hyper event log files to two.
        processParameters.put("log_file_max_count", "2");
        // Limits the size of Hyper event log files to 100 megabytes.
        processParameters.put("log_file_size_limit", "100M");

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, "example", processParameters)) {
            // Optional connection parameters. They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
            // (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
            Map<String, String> connectionParameters = new HashMap<>();
            connectionParameters.put("lc_time", "en_US");

            // Creates new Hyper file "customer.hyper"
            // Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists
            try (Connection connection = new Connection(process.getEndpoint(),
                    customerDatabasePath.toString(),
                    CreateMode.CREATE_AND_REPLACE,
                    connectionParameters)) {

                connection.getCatalog().createTable(CUSTOMER_TABLE);

                // Path that locates CSV file packaged with these examples
                Path customerCSVPath = resolveExampleFile("customers.csv");

                // Load all rows into "Customer" table from the CSV file
                // executeCommand executes a SQL statement and returns the impacted row count.
                //
                // Note:
                // You might have to adjust the COPY parameters to the format of your specific csv file.
                // The example assumes that your columns are separated with the ',' character
                // and that NULL values are encoded via the string 'NULL'.
                // Also be aware that the `header` option is used in this example:
                // It treats the first line of the csv file as a header and does not import it.
                //
                // The parameters of the COPY command are documented in the Tableau Hyper SQL documentation
                // (https:#help.tableau.com/current/api/hyper_api/en-us/reference/sql/sql-copy.html).
                System.out.println("Issuing the SQL COPY command to load the csv file into the table. Since the first line");
                System.out.println("of our csv file contains the column names, we use the `header` option to skip it.");
                long countInCustomerTable = connection.executeCommand(
                        "COPY " + CUSTOMER_TABLE.getTableName() +
                                " FROM " + escapeStringLiteral(customerCSVPath.toString()) +
                                " WITH (format csv, NULL 'NULL', delimiter ',', header)"
                ).getAsLong();

                System.out.println("The number of rows in table " + CUSTOMER_TABLE.getTableName() + " is " + countInCustomerTable + "\n");
            }
            System.out.println("The connection to the Hyper file has been closed");
        }
        System.out.println("The Hyper process has been shut down");
    }

    /**
     * Resolve the example file
     *
     * @param filename The filename
     * @return A path to the resolved file
     */
    private static Path resolveExampleFile(String filename) {
        for (Path path = Paths.get(".").toAbsolutePath(); path != null; path = path.getParent()) {
            Path file = path.resolve("data/" + filename);
            if (Files.isRegularFile(file)) {
                return file;
            }
        }
        throw new IllegalAccessError("Could not find example file. Check the working directory.");
    }
}
