<img src="https://cdns.tblsft.com/sites/default/files/blog/hyper_logo_1.jpg" width="100" height="100">


# Hyper API Samples
[![Tableau Supported](https://img.shields.io/badge/Support%20Level-Tableau%20Supported-53bd92.svg)](https://www.tableau.com/support-levels-it-and-developer-tools)

__Current Version:__ 0.0.10622

This repo is the home of the Hyper API sample code. All of the samples are available for each language supported by the Hyper API: Python, Java, C++, and C#/.Net (.NET Standard 2.0). 

If you are looking to learn more about the Hyper API, please check out the [official documentation](https://help.tableau.com/current/api/hyper_api/en-us/index.html). 



## What is the Hyper API?
For the unfamiliar, the Hyper API contains a set of functions you can use to automate your interactions with Tableau extract (.hyper) files. You can use the API to create new extract files, or to open existing files, and then insert, delete, update, or read data from those files. Using the Hyper API developers and administrators can:
* Create extract files for data sources not currently supported by Tableau.
* Automate custom extract, transform and load (ETL) processes (for example, implement rolling window updates or custom incremental updates).
* Retrieve data from an extract file.



## What do these samples do?
There are currently eight samples that are available in each language supported:

* __create_hyper_file_from_csv__
  * Demonstrates how you can use the Hyper SQL COPY command to quickly populate a table in the .hyper file from the contents of a comma-separated value (CSV) file. This technique is the fastest way to bring data into an extract, as Hyper is reading data directly from the CSV file.

* __delete_data_from_existing_hyper_file__
  * Demonstrates how you can use the Hyper SQL DELETE command to remove data from a table in the .hyper file.

* __insert_data_into_multiple_tables__
  * This example shows how you can create and insert data into a .hyper file that contains multiple tables. Uses the Inserter class to add data to the tables and a Hyper SQL query to report the number of rows in the tables.

* __insert_data_into_single_table__
  * Demonstrates how you to add data to a single table named Extract that uses the Extract schema. Uses the Inserter class to add data to the table and a Hyper SQL query to report the number of rows in the table.

* __read_and_print_data_from_existing_hyper_file__
  * Demonstrates how to read data from an existing .hyper file and print the output to the console.

* __update_data_in_existing_hyper_file__
  * This example demonstrates how you can use the Hyper SQL UPDATE command to change values in a .hyper file.

* __insert_spatial_data_to_a_hyper_file__
  * This example demonstrates how to insert spatial data (WKT) to a .hyper file

* __insert_data_using_expressions__
  * This example shows how you can use SQL expressions in Hyper API Inserter to transform or compute data on the fly during data insertion

* __defragment_data_of_existing_hyper_file__

  [![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-457387.svg)](https://www.tableau.com/support-levels-it-and-developer-tools)
  * (Python only) This example shows how you can optimize the file storage of an existing `.hyper` file by copying all of the tables and data into a new file, thereby reducing file fragmentation.

## How do I install the Hyper API?
It is a prerequisite that to work with these code samples, the Hyper API is installed in your language of choice. Head to our [official Hyper API Documentation](https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_installing.html) to get it up and running.



## How do I get help or give feedback?
If you have questions, want to submit ideas, or give feedback on the code, please do so by submitting an issue on this project.



## Contributions
Code contributions and improvements by the community are welcomed and accepted on a case-by-case basis. See the LICENSE file for current open-source licensing and use information.

Before we can accept pull requests from contributors, we require a signed [Contributor License Agreement (CLA)](https://tableau.github.io/contributing.html).
