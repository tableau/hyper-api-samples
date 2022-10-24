# git-to-hyper
## __git_to_hyper__

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

__Current Version__: 1.0

This sample shows you how use Hyper and Tableau to gain insights into a software project using the git meta data (e.g. commit timestamps). The  script extracts this data from a given git repository and stores is in a `git.hyper` file which can be used in Tableau.

Here are some examples for the [bazel-remote cache](https://github.com/buchgr/bazel-remote) OSS project:

![Tableau plot of number of ICs](ics.png)

![Tableau plot of number of SLOC](sloc.png)


## Prerequisites

To run the script, you will need:

- a computer running Windows, macOS, or Linux

- Python 3.9+

- install the dependencies from the `requirements.txt` file

## Run the basic sample

```bash
$ python basic-git-to-hyper.py --path_to_repo ~/sample/repository
```

The basic sample is a very lightweight example of how to extract meta data from git and save it into a Hyper database. It extracts only very high-level information like timestamps of commits and such. It runs in a single process.

The script offers the following options:

```bash
$ python basic-git-to-hyper.py --help
Usage: basic-git-to-hyper.py [OPTIONS]

  Extracting meta data of git repository into HYPER file.

Options:
  --path_to_repo TEXT  Path to the repository, e.g. ~/src/repo  [required]
  --branch TEXT        Branch to follow in the repository. Default: main
  --help               Show this message and exit.
```

The script will generate a `git.hyper` file which can be used in Tableau for further analysis.

## Run the advanced sample

```bash
$ python advanced-git-to-hyper.py --path_to_repo ~/sample/repository
```

The advanced sample gathers further data by e.g. running `git blame` for every changed file to count the SLOC for every author. In order to speed this up it is using multiple processes for the data extraction and a single injection process for the communication to the Hyper database.

Even if you are not interested into the git meta data this sample can still be of interest to you if you are looking for ways how to speed up your data extraction by using multiple processes.

The script offers the following options:

```bash
$ python advanced-git-to-hyper.py --help
Usage: advanced-git-to-hyper.py [OPTIONS]

  Extracting meta data of git repository into HYPER file.

Options:
  --path_to_repo TEXT        Path to the repository, e.g. ~/src/repo
                             [required]
  --ram_disk_dir TEXT        Path to ram disk on the host machine. The default
                             (/dev/shm) should work out-of-the-box for most
                             Linux OS, if you are using a different OS you
                             might need to create the ram disk manually first.
                             It needs to have at least the size of the
                             repository.
  --branch TEXT              Branch to follow in the repository. Default: main
  --number_of_workers TEXT   How many parallel processes shall be used for the
                             data extraction. Default: 1/3 of cpu_count()
  --file_size_limit INTEGER  Files bigger than this limit are not analyzed.
                             The unit is byte. Can be turned off by setting it
                             to None. Default: 10 MB
  --blame_only_for_head      Run git blame only for the HEAD commit to speed
                             up the data collection  [default: False]
  --verbose                  Increase verbosity, e.g. print filenames of git
                             blame targets  [default: False]
  --help                     Show this message and exit.
```

The script will generate a `git.hyper` file which can be used in Tableau for further analysis.

## __Resources__
Check out these resources to learn more:

- [Hyper API docs](https://help.tableau.com/current/api/hyper_api/en-us/index.html)

- [Tableau Hyper API Reference (Python)](https://help.tableau.com/current/api/hyper_api/en-us/reference/py/index.html)