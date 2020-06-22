# defragment-data-of-existing-hyper-file
## __defragment_data_of_existing_hyper_file__



![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

This sample demonstrates how you can optimize the file storage of an existing `.hyper` file by copying all of the tables and data into a new file, in a continuous sequence, to eliminate any fragmentation that might have occurred. This method can improve the performance of your Hyper file when you have large amounts of data, as fragmentation can increase both file size and access times.

This sample should serve as a starting point for anyone looking for a programmatic way to reduce the fragmentation of their `.hyper` file.

For a description of how fragmentation can occur in the `.hyper` file and for ways of minimizing it's occurrence, see [Optimize Hyper File Storage](https://help.tableau.com/current/api/hyper_api/en-us/docs/hyper_api_defrag.html).

# Get started

## __Prerequisites__

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Tableau Desktop v10.5 or later

- Python 3.6 or 3.7

## Run the sample

As a quick test of how the sample works, you don't need to make any changes to the `defragment_data_of_existing_hyper_file.py` file. You can simply run the sample on a `.hyper` file to see how it can reduce fragmentation. The Python sample reads an existing `.hyper` file and copies all the tables and data into a new file.

Ensure that you have installed the requirements and then just run the sample Python file.
The following instructions assume that you have set up a virtual environment for Python. For more information on creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html) in the Python Standard Library.

1. Open a terminal and activate the Python virtual environment (`venv`).

1. Navigate to the folder where you installed the sample.

1. Run the Python script against an existing `.hyper` file. The syntax for running the script is:
   
   **python defragment_data_of_existing_hyper_file.py [-h] [--output-hyper-file-path OUTPUT_HYPER_FILE_PATH] input_hyper_file_path**

   If you do not specify an output file, a new file is created with the same name and with `.new.hyper` as the file name extension. The new version of the file contains all the data of the existing file, without the fragmentation.

   Example:
    
    ```cli
    (venv)c:\mydir> python defragment_data_of_existing_hyper_file.py -o Newfile.hyper Oldfile.hyper
    Successfully converted table "input_database"."Extract"."Extract"
    Successfully converted Oldfile.hyper into Newfile.hyper
   ```


## __Customization__

The script, `defragment_data_of_existing_hyper_file.py`, should work "as is" in most cases. If your Hyper file contains metadata that is not stored in the table definitions, for example, column descriptions or assumed constraints, the script will still work as expected and will create a new `.hyper` file with all the tables and data. However, the new file will not contain the metadata. In that case, you might need to modify the script to preserve that information. If you created the constraints and definitions yourself, using your own SQL statements, the Python script should give you a good starting point for adding your own code.

If you use the script on an extract file created using Tableau or Prep, be aware that the metadata (if it exists) will not be copied to the new file. The script only copies the data contained in the table definitions.

To learn more about what is possible with the Hyper API, see the [official Hyper API samples](https://github.com/tableau/hyper-api-samples/tree/master/Python).

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)

- [Tableau Hyper API Reference (Python)](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/index.html)

- [Hyper API SQL Reference](https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/index.html)