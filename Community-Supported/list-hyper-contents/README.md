# list-hyper-contents

![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-53bd92.svg)

This little utility script lists the schemas, tables and columns inside a Hyper file, thereby providing you a simple first peak into a Hyper file before actually opening it, e.g., with Tableau.

This sample can serve as a starting point for anyone who wants to, e.g., programmatically enumerate all data contained inside a `.hyper` file.

But already on its own, it can be a pretty useful command line utility if you just need to knwo the table names/column names to write your own SQL queries.

# Get started

## __Prerequisites__

To run the script, you will need:

- Windows, Mac, or supported Linux distro

- Python 3.6 or 3.7

- Install tableauhyperapi: `pip install tableauhyperapi`

## How to use it

Simply call it from the console, passing the file path of your Hyper file as the first (and only) parameter like this

```
python list_hyper_contents.py my_data.hyper
```

## Exemplary usage

Running

```
python list_hyper_contents.py "World Indicators.hyper"
```

where `World Indicators.hyper` is the Hyper file from the "World Indicators" demo workbook shipped with every Tableau Desktop version, we get

```
2 schemas:
 * Schema "Extract": 1 tables
  -> Table "Extract": 26 columns
    -> "Birth Rate" DOUBLE
    -> "Business Tax Rate" DOUBLE
    -> "CO2 Emissions" BIG_INT
    -> "Country/Region" TEXT en_US
    -> "Days to Start Business" BIG_INT
    -> "Ease of Business" DOUBLE
    -> "Energy Usage" BIG_INT
    -> "GDP" DOUBLE
    -> "Health Exp % GDP" DOUBLE
    -> "Health Exp/Capita" BIG_INT
    -> "Hours to do Tax" DOUBLE
    -> "Infant Mortality Rate" DOUBLE
    -> "Internet Usage" DOUBLE
    -> "Lending Interest" DOUBLE
    -> "Life Expectancy Female" BIG_INT
    -> "Life Expectancy Male" BIG_INT
    -> "Mobile Phone Usage" DOUBLE
    -> "Population 0-14" DOUBLE
    -> "Population 15-64" DOUBLE
    -> "Population 65+" DOUBLE
    -> "Population Total" BIG_INT
    -> "Population Urban" DOUBLE
    -> "Region" TEXT en_US
    -> "Tourism Inbound" DOUBLE
    -> "Tourism Outbound" BIG_INT
    -> "Year" DATE
 * Schema "public": 0 tables
```
