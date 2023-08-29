# tde-to-hyper

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

The `.tde` file format will be deprecated by end of 2023 (see [deprecation announcement](https://community.tableau.com/s/feed/0D54T00001BHiGwSAL)).
This script upgrades `.tde` files to `.hyper` files.

# Get started

## Prerequisites

To run the script, you will need:

- A computer running Windows, macOS, or Linux
- Python (3.7 or newer)
- Install the Hyper API version `0.0.17537` (`pip install -r requirements.txt`)

## Run the sample

> **_NOTE:_**  The following command lines are for Linux or macOS and need to be slightly adapted for Windows.

The following instructions assume that you want to use a virtual environment for Python. For more information on
creating virtual environments, see [venv - Creation of virtual environments](https://docs.python.org/3/library/venv.html).

1. Open a terminal and navigate to the folder of the `tde_to_hyper.py` file
1. Create a virtual environment and install Python Hyper API
  ```
  $ python3 -m venv .venv/
  $ .venv/bin/python3 -m pip install -r requirements.txt
  ```
1. Run the Python script:
   The script requires a path to a tde file and will convert the tde to a `.hyper` file. The `.hyper` file will be created in the directory of the tde file.
   Example:
   ```cli
   $ .venv/bin/python3 tde_to_hyper.py extract.tde
   Successfully converted extract.tde to extract.hyper
   $ .venv/bin/python3 tde_to_hyper.py path/to/file/extract.tde
   Successfully converted path/to/file/extract.tde to path/to/file/extract.hyper
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
