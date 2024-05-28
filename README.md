# SQL Server  Dependency Graphing w/ADF Pipelines

This tool is meant to run and output a count of all tables, views, stored procedures, and pipelines and how they're interlinked.

A directory called `images` will be created and the logic of taking MermaidJS output to SVG to DrawIO files.

## Requirements

- `pip install requirements.txt`
- `npm install -g @mermaid-js/mermaid-cli`

The above two will require Python and NodeJS.

## ENV Values

PIPELINE_DIR needs to be set to the directory of whatever ADF pipelines you'd like to parse.
Typically ADF's version control has the name of the repo and then a directory called pipeline.

If you need to create extra output, set DEBUG="True".

## Logic

By running the queries within `queries.sql`, you can retrieve and export CSV data found in the `data` directory that lists the MS SQL Server's tables, views, and stored procedures.

These values are cross referenced, since stored procedures can reference tables and views, and views can reference tables.

Azure Data Factory can have pipelines that call tables and stored procedures directly.
ADF thankfully has version control so the repo can be downloaded locally and the JSON files of the `pipelines` directory can be parsed.

The Azure Data Factory directory should be listed within the `.env` file.
