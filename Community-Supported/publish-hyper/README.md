# publish-hyper
## __Publishing a Single-Table Hyper File Directly to Tableau Online/Server__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

This sample demonstrates how to leverage the Hyper API and Tableau Server Client Library to do the following:
- Create a single-table `.hyper` file
- Publish the file as a datasource directly to Tableau Server or Tableau Online, without a .tdsx file

It should serve as a starting point for anyone looking to automate the publishing process of (single-table) hyper files to Tableau Server or Online. 

## __Prerequisites__
To run the script, you will need:
- Windows, Mac, or supported Linux distro
- Python 3.6 - 3.7
- Run `pip install -r requirements.txt`
- Tableau Online/Server credentials or Personal Access Token

## __How to Use__
Edit the following:
- Tableau Online or Server address, site, and project
- Tableau Online or Server authentication credentials
- Name of `.hyper` file
- TableDefinition (columns and SQLTypes)

Next, you'll need to determine how to insert the data into the `.hyper` file. This will vary depending on the shape of the data and how it is stored. Please see our other samples for more on best practices with the Hyper API. Make those changes in the `insert_data()` function.
