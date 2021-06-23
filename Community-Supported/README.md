# Community Supported Samples
[![Community Supported](https://img.shields.io/badge/Support%20Level-Community%20Supported-457387.svg)](https://www.tableau.com/support-levels-it-and-developer-tools)

The community samples focus on individual use cases and are Python-only. They have been written by members of the Tableau development team but recieve the level of 'Community Supported' as they may not be maintained in newer releases. Each of the samples has been manually tested and reviewed before publishing and will still be open to pull requests and issues.

</br>

## What samples are available?
- [__adjust-vertex-order__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/adjust-vertex-order)
  - Demonstrates how to adjust the vertex order of all polygons in a `.hyper` file by copying all of the tables and data and calling transformative SQL function on all columns of type `GEOGRAPHY`.
- [__clouddb-extractor__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/clouddb-extractor)
  - Cloud Database Extractor Utility - This sample shows how to extract data from a cloud database to a published `.hyper` extract and append/update/delete rows to keep up it to date.
- [__convert_hyper_file__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/convert-hyper-file)
  - Demonstrates how to convert an existing `.hyper` file to a newer or older Hyper file format by copying all of the tables and data into a new file.
- [__defragment_data_of_existing_hyper_file__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/defragment-hyper-file)
  - Demonstrates how to optimize the file storage of an existing `.hyper` file by copying all of the tables and data into a new file to reduce file fragmentation.
- [__hyper-jupyter-kernel__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/hyper-jupyter-kernel)
  - A simple command-line-interface allowing you to inspect your Hyper files and quickly try SQL queries. Primarily helpful for debugging and ad-hoc analysis of Hyper files.
- [__hyper-to-csv__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/hyper-to-csv)
  - Shows a simple conversion of a `.hyper` database to a CSV file.
- [__list-hyper-contents__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/list-hyper-contents)
  - Lists all schemas/tables/columns contained within a `.hyper` file.
- [__parquet-to-hyper__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/parquet-to-hyper)
  - Demonstrates how to create a `.hyper` file from an Apache Parquet file.
- [__publish-hyper__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/publish-hyper)
  - Simple example of publishing single-table `.hyper` file.
- [__publish-multi-table-hyper__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/publish-multi-table-hyper)
  - Demonstrates the full end-to-end workflow of how to create a multi-table `.hyper` file, place the extract into a `.tdsx`, and publish to Tableau Online or Server.
- [__s3-to-hyper__](https://github.com/tableau/hyper-api-samples/tree/main/Community-Supported/s3-to-hyper)
  - Demonstrates how to create a `.hyper` file from a wildcard union on text files held in an AWS S3 bucket. The extract is then placed in a `.tdsx` file and published to Tableau Online or Server.


</br>
</br>

## How do I get help or give feedback?
If you have questions, want to submit ideas, or give feedback on the code, please do so by submitting an [issue on this project](https://github.com/tableau/hyper-api-samples/issues). Our team regularly monitors these issues.

## Contributions
Code contributions and improvements by the community are welcomed and accepted on a case-by-case basis. See the LICENSE file for current open-source licensing and use information.

Before we can accept pull requests from contributors, we require a signed [Contributor License Agreement (CLA)](https://tableau.github.io/contributing.html).
