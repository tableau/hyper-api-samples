// -----------------------------------------------------------------------------
//
// This file is the copyrighted property of Tableau Software and is protected
// by registered patents and other applicable U.S. and international laws and
// regulations.
//
// You may adapt this file and modify it to fit into your context and use it
// as a template to start your own projects.
//
// -----------------------------------------------------------------------------

/**
 * \example read_and_print_data_from_existing_hyper_file.cpp
 *
 * An example of how to read and print data from an existing Hyper file.
 */

#include <fstream>
#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>
#include <unordered_set>

/**
 * Helper function to copy a file
 */
static void copy(const std::string& sourcePath, const std::string& destinationPath) {
   std::ifstream source(sourcePath, std::ios::binary);
   std::ofstream destination(destinationPath, std::ios::binary);
   destination << source.rdbuf();
   source.close();
   destination.close();
}

static void runReadAndPrintDataFromExistingHyperFile() {
   std::cout << "EXAMPLE - Read data from an existing Hyper file" << std::endl;

   // Path to a Hyper file containing all data inserted into "Extract"."Extract" table
   // See "insert_data_into_single_table.cpp" for an example that works with the complete schema.
   const std::string pathToSourceDatabase = "data/superstore_sample_denormalized.hyper";

   // Make a copy of the superstore example Hyper file.
   const std::string pathToDatabase = "data/superstore_sample_denormalized_read.hyper";
   copy(pathToSourceDatabase, pathToDatabase);

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);

      // Connect to existing Hyper file "superstore_sample_denormalized_read.hyper".
      {
         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase);
         const hyperapi::Catalog& catalog = connection.getCatalog();

         // The table names in the "Extract" schema.
         std::unordered_set<hyperapi::TableName> tableNames = catalog.getTableNames("Extract");
         for (auto& tableName : tableNames) {
            hyperapi::TableDefinition tableDefinition = catalog.getTableDefinition(tableName);
            std::cout << "Table " << tableName << " has qualified name: " << tableDefinition.getTableName() << std::endl;
            for (auto& column : tableDefinition.getColumns()) {
               std::cout << "\t Column " << column.getName() << " has type " << column.getType() << " and nullability " << column.getNullability()
                         << std::endl;
            }
            std::cout << std::endl;
         }

         // Print all rows from the "Extract"."Extract" table.
         hyperapi::TableName extractTable("Extract", "Extract");
         std::cout << "These are all rows in the table " << extractTable.toString() << ":" << std::endl;

         hyperapi::Result rowsInTable = connection.executeQuery("SELECT * FROM " + extractTable.toString());
         for (const hyperapi::Row& row : rowsInTable) {
            for (const hyperapi::Value& value : row) {
               std::cout << value << '\t';
            }
            std::cout << '\n';
         }
      }
      std::cout << "The connection to the Hyper file has been closed." << std::endl;
   }
   std::cout << "The Hyper Process has been shut down." << std::endl;
}

int main() {
   try {
      runReadAndPrintDataFromExistingHyperFile();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
