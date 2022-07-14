/**
 * \example delete_data_in_existing_hyper_file.cpp
 *
 * An example of how to delete data in an existing Hyper file.
 */

#include <fstream>
#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>

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

static void runDeleteDataInExistingHyperFile() {
   std::cout << "EXAMPLE - Delete data from an existing Hyper file" << std::endl;

   // Path to a Hyper file containing all data inserted into Customer, Product, Orders and LineItems table
   // See "insert_data_into_multiple_tables.cpp" for an example that works with the complete schema.
   const std::string pathToSourceDatabase = "data/superstore_sample.hyper";

   // Make a copy of the superstore example Hyper file.
   const std::string pathToDatabase = "data/superstore_sample_delete.hyper";
   copy(pathToSourceDatabase, pathToDatabase);

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);

      // Connect to existing Hyper file "superstore_sample_delete.hyper".
      {
         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase);

         std::cout << "Delete all rows from customer with the name 'Dennis Kane' from table " << hyperapi::escapeName("Orders") << "." << std::endl;
         // `executeCommand` executes a SQL statement and returns the impacted row count.
         int64_t rowCount = connection.executeCommand(
            "DELETE FROM " + hyperapi::escapeName("Orders") + " WHERE " + hyperapi::escapeName("Customer ID") + " = ANY(SELECT " +
            hyperapi::escapeName("Customer ID") + " FROM " + hyperapi::escapeName("Customer") + " WHERE " + hyperapi::escapeName("Customer Name") + " = " +
            hyperapi::escapeStringLiteral("Dennis Kane") + ")");
         std::cout << "The number of deleted rows in table " << hyperapi::escapeName("Orders") << " is " << rowCount << "." << std::endl
                   << std::endl;

         std::cout << "Delete all rows from customer with the name 'Dennis Kane' from table " << hyperapi::escapeName("Customer") << "." << std::endl;
         rowCount = connection.executeCommand(
            "DELETE FROM " + hyperapi::escapeName("Customer") + " WHERE " + hyperapi::escapeName("Customer Name") + " =" +
            hyperapi::escapeStringLiteral("Dennis Kane"));

         std::cout << "The number of deleted rows in table Customer is " << rowCount << "." << std::endl;
      }
      std::cout << "The connection to the Hyper file has been closed." << std::endl;
   }
   std::cout << "The Hyper Process has been shut down." << std::endl;
}

int main() {
   try {
      runDeleteDataInExistingHyperFile();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
