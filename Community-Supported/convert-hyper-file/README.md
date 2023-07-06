# convert-hyper-file
## __convert_hyper_file__



![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

This sample demonstrates how you can upgrade or downgrade an existing `.hyper` file to a newer or older Hyper file format by copying all of the tables and data into a new file.

Fore more information on the Hyper file formats, see [Hyper Process Settings](https://tableau.github.io/hyper-db/docs/hyper-api/hyper_process#process-settings).

# Get started

## __Prerequisites__

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Python 3.6 or 3.7

## Run the sample

As a quick test of how the sample works, you don't need to make any changes to the `convert_hyper_file.py` file. You can simply run the sample on a `.hyper` file to see how it downgrades a Hyper file to the initial file format version 0. The Python sample reads an existing `.hyper` file and copies all the tables and data into a new file.

Ensure that you have installed the requirements and then just run the sample Python file.
The following instructions assume that you have set up a virtual environment for Python. For more information on creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html) in the Python Standard Library.

1. Open a terminal and activate the Python virtual environment (`venv`).

1. Navigate to the folder where you installed the sample.

1. Run the Python script against an existing `.hyper` file. The syntax for running the script is:
   
   **python convert_hyper_file.py [-h] [--output-hyper-file-path OUTPUT_HYPER_FILE_PATH] [--output-hyper-file-version OUTPUT_HYPER_FILE_VERSION] input_hyper_file_path**

   If you do not specify an output file, a new file is created with the same name and with `.[new version].hyper` as the file name extension. The new version of the file contains all the data of the existing file with the specified version.

   Example:
    
    ```cli
    (venv)c:\mydir> python convert_hyper_file.py -o NewVersionFile.hyper -v 1 OldVersionFile.hyper
    Successfully converted table "input_database"."Extract"."Extract"
    Successfully converted OldVersionFile.hyper into NewVersionFile.hyper
   ```

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://tableau.github.io/hyper-db)

- [Tableau Hyper API Reference (Python)](https://tableau.github.io/hyper-db/lang_docs/py/index.html)

- [Hyper API SQL Reference](https://tableau.github.io/hyper-db/docs/sql/)
