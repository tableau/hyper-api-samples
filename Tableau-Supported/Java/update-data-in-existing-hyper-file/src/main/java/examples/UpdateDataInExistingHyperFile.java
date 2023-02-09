package examples;

import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Result;
import com.tableau.hyperapi.ResultSchema;
import com.tableau.hyperapi.Telemetry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static com.tableau.hyperapi.Sql.escapeName;
import static com.tableau.hyperapi.Sql.escapeStringLiteral;

/**
 * An example demonstrating how to update data in an existing Hyper file
 */
public class UpdateDataInExistingHyperFile {
    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Update existing data in a Hyper file\n");

        // Path to a Hyper file containing all data inserted into Customer, Product, Orders and LineItems table
        Path pathToOriginalDatabase = resolveExampleFile("superstore_sample.hyper");

        // Make a copy of the superstore sample Hyper file
        Path pathToCopiedDatabase = Paths.get(getWorkingDirectory(), "superstore_sample_update.hyper").toAbsolutePath();
        try {
            Files.copy(pathToOriginalDatabase, pathToCopiedDatabase, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {

            // Connect to existing file "superstore_sample_update.hyper""
            try (Connection connection = new Connection(process.getEndpoint(),
                    pathToCopiedDatabase.toString())) {

                String selectCustomerTableQuery = "SELECT " + escapeName("Loyalty Reward Points") + "," +
                        escapeName("Segment") + "FROM " + escapeName("Customer");

                try (Result rowsPreUpdate = connection.executeQuery(selectCustomerTableQuery)) {
                    // Placing executeQuery in a try block ensures all results associated with the connection
                    // are closed before attempting a new operation on it. Otherwise, the connection would still
                    // be open when calling executeCommand
                    System.out.println("Pre-Update: Individual rows showing 'Loyalty Reward Points' and 'Segment' columns:");

                    ResultSchema resultSchema = rowsPreUpdate.getSchema();
                    while (rowsPreUpdate.nextRow()) {
                        System.out.println("{" +
                                rowsPreUpdate.getLong(resultSchema.getColumnPositionByName("Loyalty Reward Points").getAsInt()) + ", " +
                                rowsPreUpdate.getString(resultSchema.getColumnPositionByName("Segment").getAsInt()) +
                                "}"
                        );
                    }
                }
                System.out.println();

                System.out.println("Update 'Customers' table by adding 50 Loyalty Reward Points to all Corporate Customers");
                long updatedRowCount = connection.executeCommand(
                        "UPDATE " + escapeName("Customer") + " SET " +
                                escapeName("Loyalty Reward Points") + "=" + escapeName("Loyalty Reward Points") + " + 50 " +
                                "WHERE " + escapeName("Segment") + "=" + escapeStringLiteral("Corporate")
                ).getAsLong();


                System.out.println("The number of updated rows in 'Customer' table is " + updatedRowCount + "\n");

                try (Result rowsPostUpdate = connection.executeQuery(selectCustomerTableQuery)) {
                    System.out.println("Post-Update: Individual rows showing 'Loyalty Reward Points' and 'Segment' columns:");

                    ResultSchema resultSchema = rowsPostUpdate.getSchema();
                    while (rowsPostUpdate.nextRow()) {
                        System.out.println("{" +
                                rowsPostUpdate.getLong(resultSchema.getColumnPositionByName("Loyalty Reward Points").getAsInt()) + ", " +
                                rowsPostUpdate.getString(resultSchema.getColumnPositionByName("Segment").getAsInt()) +
                                "}");
                    }
                    System.out.println();
                }
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
        for (Path path = Paths.get(getWorkingDirectory()).toAbsolutePath(); path != null; path = path.getParent()) {
            Path file = path.resolve("data/" + filename);
            if (Files.isRegularFile(file)) {
                return file;
            }
        }
        throw new IllegalAccessError("Could not find example file. Check the working directory.");
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
