//---------------------------------------------------------------------------
//
// This file is the copyrighted property of Tableau Software and is protected
// by registered patents and other applicable U.S. and international laws and
// regulations.
//
// Unlicensed use of the contents of this file is prohibited. Please refer to
// the NOTICES.txt file for further details.
//
//---------------------------------------------------------------------------
package examples;

import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.HyperProcess;
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
 * An example demonstrating deleting data from an existing Hyper file
 */
public class DeleteDataInExistingHyperFile {
    /**
     * The main function
     *
     * @param args The args
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Delete data from an existing Hyper file\n");

        // Path to an Hyper file containing all data inserted into Customer, Product, Orders and LineItems table
        Path pathToOriginalDatabase = resolveExampleFile("superstore_sample.hyper");

        // Make a copy of the superstore example Hyper file
        Path pathToCopiedDatabase = Paths.get("superstore_sample_delete.hyper").toAbsolutePath();
        try {
            Files.copy(pathToOriginalDatabase, pathToCopiedDatabase, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {

            // Connect to existing Hyper file "superstore_sample_delete.hyper"
            try (Connection connection = new Connection(process.getEndpoint(),
                    pathToCopiedDatabase.toString())) {

                System.out.println("Delete all rows from customer with the name 'Dennis Kane' from 'Orders' table");

                // executeCommand executes a SQL statement and returns the impacted row count
                long rowCountDeletedInOrdersTable = connection.executeCommand(
                        "DELETE FROM " + escapeName("Orders") + " WHERE " + escapeName("Customer ID") +
                                "= ANY(" +
                                "SELECT " + escapeName("Customer ID") + " FROM " + escapeName("Customer") +
                                " WHERE " + escapeName("Customer Name") + "=" + escapeStringLiteral("Dennis Kane") +
                                ")"
                ).getAsLong();

                System.out.println("The number of deleted rows in table 'Orders' is " + rowCountDeletedInOrdersTable + "\n");

                System.out.println("Delete all rows from customer with the name 'Dennis Kane' from 'Customer' table");
                long rowCountDeletedInCustomersTable = connection.executeCommand(
                        "DELETE FROM " + escapeName("Customer") +
                                "WHERE " + escapeName("Customer Name") + "=" + escapeStringLiteral("Dennis Kane")
                ).getAsLong();

                System.out.println("The number of deleted rows in table 'Customer' is " + rowCountDeletedInCustomersTable + "\n");
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
