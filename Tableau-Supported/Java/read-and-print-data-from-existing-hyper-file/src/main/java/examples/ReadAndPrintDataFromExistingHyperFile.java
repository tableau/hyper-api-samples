package examples;

import com.tableau.hyperapi.Catalog;
import com.tableau.hyperapi.Connection;
import com.tableau.hyperapi.HyperProcess;
import com.tableau.hyperapi.Name;
import com.tableau.hyperapi.SchemaName;
import com.tableau.hyperapi.TableName;
import com.tableau.hyperapi.Result;
import com.tableau.hyperapi.ResultSchema;
import com.tableau.hyperapi.TableDefinition;
import com.tableau.hyperapi.Telemetry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static com.tableau.hyperapi.Sql.escapeName;

/**
 * An example demonstrating how to read data from an existing Hyper file and how to print data from its tables.
 */
public class ReadAndPrintDataFromExistingHyperFile {
    /**
     * The main function.
     *
     * @param args The args.
     */
    public static void main(String[] args) {
        System.out.println("EXAMPLE - Read data from an existing Hyper file\n");

        // Path to an Hyper file containing all data inserted into Customer, Product, Orders and LineItems table
        Path pathToOriginalDatabase = resolveExampleFile("superstore_sample_denormalized.hyper");

        // Make a copy of the superstore example Hyper file
        Path pathToCopiedDatabase = Paths.get(getWorkingDirectory(), "superstore_sample_denormalized_read.hyper").toAbsolutePath();
        try {
            Files.copy(pathToOriginalDatabase, pathToCopiedDatabase, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Starts the Hyper Process with telemetry enabled to send data to Tableau.
        // To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
        try (HyperProcess process = new HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) {

            // Connect to existing Hyper file "superstore_sample_read.hyper"
            try (Connection connection = new Connection(process.getEndpoint(),
                    pathToCopiedDatabase.toString())) {
                Catalog catalog = connection.getCatalog();

                // Get all tables in "Extract" schema of the Hyper file
                SchemaName extractSchema = new SchemaName("Extract");
                List<TableName> tablesInDatabase = catalog.getTableNames(extractSchema);

                for (TableName table : tablesInDatabase) {
                    TableDefinition tableDefinition = catalog.getTableDefinition(table);
                    List<TableDefinition.Column> columns = tableDefinition.getColumns();
                    System.out.println("Table " + table + " has qualified name: " + tableDefinition.getTableName());
                    for (TableDefinition.Column column : columns) {
                        System.out.println("Column " + column.getName() +
                                " has type=" + column.getType().getTag() +
                                " and nullability=" + column.getNullability());
                    }
                    System.out.println();
                }
                System.out.println();

                // Print all rows from the "Extract"."Extract" table
                System.out.println("These are all rows in the table 'Extract'");
                TableName tableName = new TableName(extractSchema, "Extract");
                try (Result result = connection.executeQuery("SELECT * FROM " + tableName.toString())) {
                    ResultSchema resultSchema = result.getSchema();

                    int categoryPosition = resultSchema.getColumnPositionByName("Category").getAsInt();
                    int orderDatePosition = resultSchema.getColumnPositionByName("Order Date").getAsInt();
                    int salesTargetPosition = resultSchema.getColumnPositionByName("Sales Target").getAsInt();
                    int segmentPosition = resultSchema.getColumnPositionByName("Segment").getAsInt();

                    while (result.nextRow()) {
                        System.out.println("{" +
                                result.getString(categoryPosition) + ", " +
                                result.getLocalDate(orderDatePosition) + ", " +
                                result.getLong(salesTargetPosition) + ", " +
                                result.getString(segmentPosition) +
                                "}");
                    }
                }
                System.out.println();
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
