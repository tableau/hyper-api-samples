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
 * \example insert_data_with_expressions.cpp
 *
 * An example of how to push down computations to Hyper during data insertion using expressions.
 */

#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>
#include <unordered_set>

// The table is called "Extract" and will be created in the "Extract" schema.
// This has historically been the default table name and schema for extracts created by Tableau.
static const hyperapi::TableDefinition extractTable{
   {"Extract", "Extract"},
   {hyperapi::TableDefinition::Column{"Order ID", hyperapi::SqlType::integer(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Ship Timestamp", hyperapi::SqlType::timestamp(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Ship Mode", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
    hyperapi::TableDefinition::Column{"Ship Priority", hyperapi::SqlType::integer(), hyperapi::Nullability::NotNullable}}};

static void runInsertDataWithExpressions() {
   std::cout << "EXAMPLE - Push down computations to Hyper during data insertion using expressions" << std::endl;
   const std::string pathToDatabase = "data/orders.hyper";

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);
      // Creates new Hyper file "orders.hyper".
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
         std::vector<hyperapi::TableDefinition::Column> inserterDefinition{
            hyperapi::TableDefinition::Column{"Order ID", hyperapi::SqlType::integer(), hyperapi::Nullability::NotNullable},
            hyperapi::TableDefinition::Column{"Ship Timestamp Text", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
            hyperapi::TableDefinition::Column{"Ship Mode", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
            hyperapi::TableDefinition::Column{"Ship Priority Text", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable}};

         // Column 'Order Id' is inserted into "Extract"."Extract" as-is.
         // Column 'Ship Timestamp' in "Extract"."Extract" of timestamp type is computed from Column 'Ship Timestamp Text' of text type using 'to_timestamp()'.
         // Column 'Ship Mode' is inserted into "Extract"."Extract" as-is.
         // Column 'Ship Priority' is "Extract"."Extract" of integer type is computed from Colum 'Ship Priority Text' of text type using 'CASE' statement.
         std::string textToTimeStampExpression = "to_timestamp(" + hyperapi::escapeName("Ship Timestamp Text") + ", " + hyperapi::escapeStringLiteral("YYYY-MM-DD HH24:MI:SS") + ")";
         std::string shipPriorityAsIntCaseExpression = "CASE " + hyperapi::escapeName("Ship Priority Text") +
            " WHEN " + hyperapi::escapeStringLiteral("Urgent") + " THEN 1 " +
            " WHEN " + hyperapi::escapeStringLiteral("Medium") + " THEN 2 " +
            " WHEN " + hyperapi::escapeStringLiteral("Low") + " THEN 3 END";

         std::vector<hyperapi::Inserter::ColumnMapping> columnMappings{
            hyperapi::Inserter::ColumnMapping{"Order ID"},
            hyperapi::Inserter::ColumnMapping{"Ship Timestamp", textToTimeStampExpression},
            hyperapi::Inserter::ColumnMapping{"Ship Mode"},
            hyperapi::Inserter::ColumnMapping{"Ship Priority", shipPriorityAsIntCaseExpression}};

         // Insert data into the "Extract"."Extract" table using expressions.
         {
            hyperapi::Inserter inserter(connection, extractTable, columnMappings, inserterDefinition);
            inserter.addRow(399, "2012-09-13 10:00:00", "Express Class", "Urgent");
            inserter.addRow(530, "2012-07-12 14:00:00", "Standard Class", "Low");
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
      runInsertDataWithExpressions();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
