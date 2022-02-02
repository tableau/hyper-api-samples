# tde-to-hyper
## __tde_to_hyper__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

This script can be used to create a `.hyper` file from a `.tde` file.

# Get started

## __Prerequisites__

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Python (3.7 or newer)

- Install the Hyper API `pip install -r requirements.txt`

## Run the sample

Ensure that you have installed the requirements and then just run the sample Python file.
The following instructions assume that you have set up a virtual environment for Python. For more information on
creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html)
in the Python Standard Library.

1. Open a terminal and activate the Python virtual environment (`venv`).

1. Navigate to the folder where you installed the sample.

1. Run the Python script:
   
   **python tde_to_hyper.py input_tde_path**

   The script requires a path to a tde file and will convert the tde to a `.hyper` file. The `.hyper` file will be created in the directory of the tde file.

   Example:

   ```cli
   (venv)c:\mydir> python tde_to_hyper.py extract.tde
   Successfully converted extract.tde to extract.hyper

   (venv)c:\mydir> python tde_to_hyper.py c:\path\to\file\extract.tde
   Successfully converted c:\path\to\file\extract.tde to c:\path\to\file\extract.hyper
   ```

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)

- [Tableau Hyper API Reference (Python)](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/index.html)
