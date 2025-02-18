/**
 * \example insert_geospatial_data_to_a_hyper_file.cpp
 *
 * An example of how to insert geospatial data into a single-table Hyper file.
 */

#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>
#include <unordered_set>

// The table is called "Extract" and will be created in the "Extract" schema.
// This has historically been the default table name and schema for extracts created by Tableau.
static const hyperapi::TableDefinition extractTable{
   {"Extract", "Extract"},
   {hyperapi::TableDefinition::Column{"Name", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Location", hyperapi::SqlType::tabgeography(), hyperapi::Nullability::NotNullable}}};

static void runInsertSpatialDataToAHyperFile() {
   std::cout << "EXAMPLE - Insert geospatial data into a single table within a new Hyper file" << std::endl;
   const std::string pathToDatabase = "data/spatial_data.hyper";

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);
      // Creates new Hyper file "spatial_data.hyper".
      // Replaces existing file with hyperapi::CreateMode::CreateAndReplace if it already exists.
      {
         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase, hyperapi::CreateMode::CreateAndReplace);
         const hyperapi::Catalog& catalog = connection.getCatalog();

         // Create the schema and the table.
         catalog.createSchema("Extract");
         catalog.createTable(extractTable);

         // Hyper API's Inserter allows users to transform data during insertion.
         // To make use of data transformation during insertion, the inserter requires the following inputs
         //   1. The connection to the Hyper instance containing the table
         //   2. The table name or table defintion into which data is inserted
         //   3. List of hyperapi::Inserter::ColumnMapping.
         //       This list informs the inserter how each column in the target table is tranformed.
         //       The list must contain all the columns into which data is inserted.
         //       "hyperapi::Inserter::ColumnMapping" maps a valid SQL expression (if any) to a column in the target table
         //       For example hyperapi::Inserter::ColumnMapping("target_column", hyperapi::escapeName("colA") + "*" + hyperapi::escapeName("colB"))
         //       The column "target_column" contains the product of "colA" and "colB" after successful insertion.
         //       SQL expression string is optional in Inserter.ColumnMapping.
         //       For a column without any transformation only the column name is required.
         //       For example hyperapi::Inserter::ColumnMapping{"no_data_transformation_column"}
         //   4. Inserter Definition, a list of column definitions for all the input values provided during insertion.

         // Inserter definition contains the column definition for the values that are inserted.
         // The data input has two text values Name and Location_as_text.
         std::vector<hyperapi::TableDefinition::Column> inserterDefinition{
            hyperapi::TableDefinition::Column{"Name", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
            hyperapi::TableDefinition::Column{"Location_as_text", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable}};

         // Column 'Name' is inserted into "Extract"."Extract" as-is.
         // Column 'Location' in "Extract"."Extract" of `tableau.tabgeography` type is computed from Column 'Location_as_text' of `text` type
         // using the expression 'CAST("Location_as_text") AS TABLEAU.TABGEOGRAPHY'.
         // hyperapi::Inserter::ColumnMapping is used for mapping the CAST expression to Column 'Location'.
         std::string textToGeographyCastExpression = "CAST(" + hyperapi::escapeName("Location_as_text") + " AS TABLEAU.TABGEOGRAPHY)";
         std::vector<hyperapi::Inserter::ColumnMapping> columnMappings{
            hyperapi::Inserter::ColumnMapping{"Name"},
            hyperapi::Inserter::ColumnMapping{"Location", textToGeographyCastExpression}};

         // Insert geospatial data into the "Extract"."Extract" table using CAST expression.
         {
            hyperapi::Inserter inserter(connection, extractTable, columnMappings, inserterDefinition);
            inserter.addRow("Seattle", "point(-122.338083 47.647528)");
            inserter.addRow("Munich", "point(11.584329 48.139257)");
            inserter.execute();
         }

         // Number of rows in the "Extract"."Extract" table.
         // `executeScalarQuery` is for executing a query that returns exactly one row with one column.
         int64_t rowCount = connection.executeScalarQuery<int64_t>("SELECT COUNT(*) FROM " + extractTable.getTableName().toString());
         std::cout << "The number of rows in table " << extractTable.getTableName() << " is " << rowCount << "." << std::endl;
      }
      std::cout << "The connection to the Hyper file has been closed." << std::endl;
   }
   std::cout << "The Hyper Process has been shut down." << std::endl;
}

int main() {
   try {
      runInsertSpatialDataToAHyperFile();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
