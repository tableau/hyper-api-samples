# tde-to-hyper

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

The `.tde` file format will be deprecated by end of 2023 (see [deprecation announcement](https://community.tableau.com/s/feed/0D54T00001BHiGwSAL)).
This script upgrades `.tde` files to `.hyper` files.

# Get started

## Prerequisites

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

## Advanced usage

The script can also be invoked on a folder and will upgrade each `.tde` file within
that folder. The upgraded `.hyper` files will be placed directly next to the `.tde`
files. If the new `.hyper` files should be written to a differnt folder, the
`--output` can be used to specify a location where the upgraded `.hyper` should be
written.

## Resources

- [Hyper API docs](https://tableau.github.io/hyper-db)
- [Tableau Hyper API Reference (Python)](https://tableau.github.io/hyper-db/lang_docs/py/index.html)
- [TDE deprecation announcement](https://community.tableau.com/s/feed/0D54T00001BHiGwSAL)
- [August 2023 update on TDE deprecation](https://community.tableau.com/s/feed/0D58b0000BTEIShCQP)
