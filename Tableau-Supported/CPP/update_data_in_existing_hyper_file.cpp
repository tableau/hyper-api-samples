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

#include <fstream>
#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>

/**
 * \example update_data_in_existing_hyper_file.cpp
 *
 * An example of how to update data in an existing Hyper file.
 */

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

static void runUpdateDataInExistingHyperFile() {
   std::cout << "EXAMPLE - Update existing data in a Hyper file" << std::endl;

   // Path to a Hyper file containing all data inserted into Customer, Product, Orders and LineItems table
   // See "insert_data_into_multiple_tables.cpp" for an example that works with the complete schema.
   const std::string pathToSourceDatabase = "data/superstore_sample.hyper";

   // Make a copy of the superstore example Hyper file.
   const std::string pathToDatabase = "data/superstore_sample_update.hyper";
   copy(pathToSourceDatabase, pathToDatabase);

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);

      // Connect to existing Hyper file "superstore_sample_update.hyper".
      {
         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase);

         hyperapi::Result rowsPreUpdate = connection.executeQuery(
            "SELECT " + hyperapi::escapeName("Loyalty Reward Points") + ", " + hyperapi::escapeName("Segment") + " FROM " + hyperapi::escapeName("Customer"));
         std::cout << "Pre-Update: Individual rows showing 'Loyalty Reward Points' and 'Segment' columns: " << std::endl;
         for (const hyperapi::Row& row : rowsPreUpdate) {
            for (const hyperapi::Value& value : row) {
               std::cout << value << '\t';
            }
            std::cout << '\n';
         }
         std::cout << std::endl;

         std::cout << "Update 'Customers' table by adding 50 Loyalty Reward Points to all Corporate Customers." << std::endl;
         int64_t rowCount = connection.executeCommand(
            "UPDATE " + hyperapi::escapeName("Customer") + " SET " + hyperapi::escapeName("Loyalty Reward Points") + " = " +
            hyperapi::escapeName("Loyalty Reward Points") + " + 50 " + "WHERE " + hyperapi::escapeName("Segment") + " = " +
            hyperapi::escapeStringLiteral("Corporate"));

         std::cout << "The number of updated rows in table " << hyperapi::escapeName("Customer") << " is " << rowCount << "." << std::endl;

         hyperapi::Result rowsPostUpdate = connection.executeQuery(
            "SELECT " + hyperapi::escapeName("Loyalty Reward Points") + ", " + hyperapi::escapeName("Segment") + " FROM " + hyperapi::escapeName("Customer"));
         for (const hyperapi::Row& row : rowsPostUpdate) {
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
      runUpdateDataInExistingHyperFile();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
