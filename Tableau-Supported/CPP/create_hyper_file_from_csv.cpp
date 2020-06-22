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
 * \example create_hyper_file_from_csv.cpp
 *
 * An example of how to load data from a CSV file into a new Hyper file.
 */

#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>

static const hyperapi::TableDefinition customerTable{
   "Customer", // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
   {hyperapi::TableDefinition::Column{"Customer ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Customer Name", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Loyalty Reward Points", hyperapi::SqlType::bigInt(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Segment", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable}}};

static void runCreateHyperFileFromCSV() {
   std::cout << "EXAMPLE - Load data from CSV into table in new Hyper file" << std::endl;
   const std::string pathToDatabase = "data/customer.hyper";

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      // Optional process parameters. They are documented in the Tableau Hyper documentation, chapter "Process Settings"
      // (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
      std::unordered_map<std::string, std::string> processParameters = {
         // Limits the number of Hyper event log files to two.
         {"log_file_max_count", "2"},
         // Limits the size of Hyper event log files to 100 megabytes.
         {"log_file_size_limit", "100M"}};

      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau, "example", processParameters);
      // Creates new Hyper file "customer.hyper".
      // Replaces existing file with hyperapi::CreateMode::CreateAndReplace if it already exists.
      {
         // Optional connection parameters. They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
         // (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
         std::unordered_map<std::string, std::string> connectionParameters = {{"lc_time", "en_US"}};

         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase, hyperapi::CreateMode::CreateAndReplace, connectionParameters);
         const hyperapi::Catalog& catalog = connection.getCatalog();

         catalog.createTable(customerTable);

         // Using path to current file, create a path that locates CSV file packaged with these examples.
         std::string pathToCSV = "data/customers.csv";

         // Load all rows into "Customers" table from the CSV file.
         // `executeCommand` executes a SQL statement and returns the impacted row count.
         int64_t rowCount = connection.executeCommand(
            "COPY " + customerTable.getTableName().toString() + " from " + hyperapi::escapeStringLiteral(pathToCSV) +
            " with (format csv, NULL 'NULL', delimiter ',', header)");

         std::cout << "The number of rows in table " << customerTable.getTableName() << " is " << rowCount << "." << std::endl;
      }
      std::cout << "The connection to the Hyper file has been closed." << std::endl;
   }
   std::cout << "The Hyper Process has been shut down." << std::endl;
}

int main() {
   try {
      runCreateHyperFileFromCSV();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
