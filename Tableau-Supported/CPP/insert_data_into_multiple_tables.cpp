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
 * \example insert_data_into_multiple_tables.cpp
 *
 * An example of how to create and insert into a multi-table Hyper file with different column types.
 */

#include <hyperapi/hyperapi.hpp>
#include <iostream>
#include <string>

/** Table Definitions required to create tables */
using Column = hyperapi::TableDefinition::Column;
static const hyperapi::TableDefinition ordersTable{"Orders", // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                                                   {
                                                      Column{"Address ID", hyperapi::SqlType::smallInt(), hyperapi::Nullability::NotNullable},
                                                      Column{"Customer ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                      Column{"Order Date", hyperapi::SqlType::date(), hyperapi::Nullability::NotNullable},
                                                      Column{"Order ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                      Column{"Ship Date", hyperapi::SqlType::date(), hyperapi::Nullability::Nullable},
                                                      Column{"Ship Mode", hyperapi::SqlType::text(), hyperapi::Nullability::Nullable},
                                                   }};
static const hyperapi::TableDefinition customerTable{"Customer", // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                                                     {Column{"Customer ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                      Column{"Customer Name", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                      Column{"Loyalty Reward Points", hyperapi::SqlType::bigInt(), hyperapi::Nullability::NotNullable},
                                                      Column{"Segment", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable}}};
static const hyperapi::TableDefinition productTable{"Products", // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                                                    {
                                                       Column{"Category", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                       Column{"Product ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                       Column{"Product Name", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                       Column{"Sub-Category", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                    }};
static const hyperapi::TableDefinition lineItemsTable{"Line Items", // Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                                                      {
                                                         Column{"Line Item ID", hyperapi::SqlType::bigInt(), hyperapi::Nullability::NotNullable},
                                                         Column{"Order ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                         Column{"Product ID", hyperapi::SqlType::text(), hyperapi::Nullability::NotNullable},
                                                         Column{"Sales", hyperapi::SqlType::doublePrecision(), hyperapi::Nullability::NotNullable},
                                                         Column{"Quantity", hyperapi::SqlType::smallInt(), hyperapi::Nullability::NotNullable},
                                                         Column{"Discount", hyperapi::SqlType::doublePrecision(), hyperapi::Nullability::Nullable},
                                                         Column{"Profit", hyperapi::SqlType::doublePrecision(), hyperapi::Nullability::NotNullable},
                                                      }};

static void runInsertDataIntoMultipleTables() {
   std::cout << "EXAMPLE - Insert data into multiple tables within a new Hyper file" << std::endl;
   const std::string pathToDatabase = "data/superstore.hyper";

   // Starts the Hyper Process with telemetry enabled to send data to Tableau.
   // To opt out, simply set telemetry=hyperapi::Telemetry::DoNotSendUsageDataToTableau.
   {
      hyperapi::HyperProcess hyper(hyperapi::Telemetry::SendUsageDataToTableau);
      // Creates new Hyper file "superstore.hyper"
      // Replaces existing file with hyperapi::CreateMode::CreateAndReplace if it already exists.
      {
         hyperapi::Connection connection(hyper.getEndpoint(), pathToDatabase, hyperapi::CreateMode::CreateAndReplace);
         const hyperapi::Catalog& catalog = connection.getCatalog();

         // Create multiple tables.
         catalog.createTable(ordersTable);
         catalog.createTable(customerTable);
         catalog.createTable(productTable);
         catalog.createTable(lineItemsTable);

         // Insert data into Orders table.
         {
            hyperapi::Inserter inserter(connection, ordersTable);
            inserter.addRow(static_cast<int16_t>(399), "DK-13375", hyperapi::Date{2012, 9, 7}, "CA-2011-100006", hyperapi::Date{2012, 9, 13}, "Standard Class");
            inserter.addRow(static_cast<int16_t>(530), "EB-13705", hyperapi::Date{2012, 7, 8}, "CA-2011-100090", hyperapi::Date{2012, 7, 12}, "Standard Class");
            inserter.execute();
         }

         // Insert data into Customer table.
         {
            hyperapi::Inserter inserter(connection, customerTable);
            inserter.addRow("DK-13375", "Dennis Kane", 518, "Consumer");
            inserter.addRow("EB-13705", "Ed Braxton", 815, "Corporate");
            inserter.execute();
         }

         // Insert individual row into Product table.
         {
            hyperapi::Inserter inserter(connection, productTable);
            inserter.addRow("TEC-PH-10002075", "Technology", "Phones", "AT&T EL51110 DECT");
            inserter.execute();
         }

         // Insert data into Line Items table.
         {
            hyperapi::Inserter inserter(connection, lineItemsTable);
            inserter.addRow(2718, "CA-2011-100006", "TEC-PH-10002075", 377.97, int16_t{3}, 0.0, 109.6113);
            inserter.addRow(2719, "CA-2011-100090", "TEC-PH-10002075", 377.97, int16_t{3}, hyperapi::null, 109.6113);
            inserter.execute();
         }

         for (auto& tableName : {ordersTable.getTableName(), customerTable.getTableName(), productTable.getTableName(), lineItemsTable.getTableName()}) {
            // `executeScalarQuery` is for executing a query that returns exactly one row with one column.
            int64_t rowCount = connection.executeScalarQuery<int64_t>("SELECT COUNT(*) FROM " + tableName.toString());
            std::cout << "The number of rows in table " << tableName << " is " << rowCount << "." << std::endl;
         }
      }
      std::cout << "The connection to the Hyper file has been closed." << std::endl;
   }
   std::cout << "The Hyper Process has been shut down." << std::endl;
}

int main() {
   try {
      runInsertDataIntoMultipleTables();
   } catch (const hyperapi::HyperException& e) {
      std::cout << e.toString() << std::endl;
      return 1;
   }
   return 0;
}
