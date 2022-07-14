/**
 * \example insert_data_into_single_table.cpp
 *
 * An example of how to create and insert into a single-table Hyper file with different column types.
 */

#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>
#include <unordered_set>

// The table is called "Extract" and will be created in the "Extract" schema.
// This has historically been the default table name and schema for extracts created by Tableau.
static const hyperapi::TableDefinition extractTable{
   {"Extract", "Extract"},
   {hyperapi::TableDefinition::Column{"Customer ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Customer Name", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Loyalty Reward Points", hyperapi::SqlType::bigInt(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Segment", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable}}};

static void runInsertDataIntoSingleTable() {
   std::cout << "EXAMPLE - Insert data into a single table within a new Hyper file" << std::endl;
   const std::string pathToDatabase = "data/customer.hyper";

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);
      // Creates new Hyper file "customer.hyper".
      // Replaces existing file with hyperapi::CreateMode::CreateAndReplace if it already exists.
      {
         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase, hyperapi::CreateMode::CreateAndReplace);
         const hyperapi::Catalog& catalog = connection.getCatalog();

         // Create the schema and the table.
         catalog.createSchema("Extract");
         catalog.createTable(extractTable);

         // Insert data into the "Extract"."Extract" table.
         {
            hyperapi::Inserter inserter(connection, extractTable);
            inserter.addRow("DK-13375", "Dennis Kane", 518, "Consumer");
            inserter.addRow("EB-13705", "Ed Braxton", 815, "Corporate");
            inserter.execute();
         }

         // Print the table names in the "Extract" schema.
         std::unordered_set<hyperapi::TableName> tableNames = catalog.getTableNames("Extract");
         std::cout << "Tables available in " << pathToDatabase << " in the Extract schema are: ";
         for (auto& tableName : tableNames)
            std::cout << tableName.toString() << "\t";
         std::cout << std::endl;

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
      runInsertDataIntoSingleTable();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
