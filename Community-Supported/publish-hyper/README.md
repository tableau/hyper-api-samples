# publish-hyper
## __Publishing a Single-Table Hyper File Directly to Tableau Online/Server__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

This sample demonstrates how to leverage the Hyper API and Tableau Server Client Library to do the following:
- Create a single-table Hyper file
- Publish the file as a datasource directly to Online/Server, without a .tdsx file

It should serve as a starting point for anyone looking to automate the publishing process of multi-table extracts and data sources to Tableau. The Tableau Server Client library (TSC) is able to publish single-table hyper files directly to Online/Server, but this is not currently the case for any extract with multiple tables. Because of this, there is an important additional step of swapping the newly built hyper file into an existing packaged data source.

## __Prerequisites__
To run the script, you will need:
- Windows, Mac, or supported Linux distro
- Python 3.6 - 3.7
- Run `pip install -r requirements.txt`
- Tableau Online/Server credentials or Personal Access Token
