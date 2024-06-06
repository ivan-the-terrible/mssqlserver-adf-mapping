# SQL Server  Dependency Graphing w/ADF Pipelines

This tool is meant to run and output a count of all tables, views, stored procedures, and pipelines and how they're interlinked.

## Logic

By running the queries within `queries.sql`, you can retrieve and export CSV data that lists the MS SQL Server's tables, views, and stored procedures.

These values are cross referenced, since stored procedures can reference tables and views, and views can reference tables.

Azure Data Factory can have pipelines that call tables, stored procedures, and other pipelines.
ADF thankfully has version control so the repo can be downloaded locally and the JSON files of the `pipelines` directory can be parsed.

## Structure

```mermaidjs
project
├── data
│   └── MSSQL_SERVER_DATA_DIR
│       ├── Tables.csv
│       ├── View.csv
│       └── StoredProcedures.csv
├── images
│   └── OUTPUT_DIR
│       ├── json
│       ├── mermaid
│       └── pdf
└── reports
    └── OUTPUT_DIR
        ├── table-report.txt
        ├── view-report.txt
        ├── stored-procedures-report.txt
        └── pipeline-report.txt
```

The key directories here are: data, images, and reports.
The subdirectories within them are named after values within your `.env`.

- `data` contains the source information. It is expected that the user create this directory and another within that is then set to the `MSSQL_SERVER_DATA_DIR` env value. This subdirectory contains the CSVs saved output from `queries.sql`. Please use those queries, select all, and copy.
  - Tables.csv        -> just Copy
  - View.csv          -> Copy with Headers
  - Stored Procedures -> Copy with Headers

- `images` contains the result of taking the Mermaid format of the built trees and outputing it to PDF and JSON. This directory will be created, as will the subdirectory named after the value `OUTPUT_DIR` specified in your `.env`. The raw Mermaid output can also be output if `DEBUG` is set to `True` within your `.env`.

- `reports` contains the text file reports. This directory will be created, as will the subdirectory named after the value `OUTPUT_DIR` specified in your `.env`. The text files contain reference counts and a summary of the linked references.

## Requirements

The below will require Python and NodeJS.
(Last used with Python 3.12.3 and Node 22.2.0)

You can use the `install.ps1` script, which will install a Python Virtual Environment and the Mermaid CLI npm package.
Other than activating the environment, these commands are OS agnostic as well.

- `pip install requirements.txt`
- `npm install -g @mermaid-js/mermaid-cli`

Mermaid CLI repo: <https://github.com/mermaid-js/mermaid-cli>

Invoking the script is as easy as `python report.py`.

## ENV Values

Create an `.env` file with the following:

- `PIPELINE_DIR` needs to be set to the directory of whatever ADF pipelines you'd like to parse.
Typically ADF's version control has the name of the repo and then a directory called pipeline.

- `MSSQL_SERVER_DATA_DIR` is the name of the folder that exists within the `data` directory that contains Tables.csv, Views.csv, and StoredProcedures.csv. The user is expected to create this folder and copy the name to this env value.

- `OUTPUT_DIR` is the name of the folder that exists within the `images` and also the `reports` directory that will be generated to contain images and reports.

- If you need to create extra output, set `DEBUG="True"` and a `debug` directory will be made with raw class values. See also the raw Mermaid output within the `images` directory as mentioned above.
