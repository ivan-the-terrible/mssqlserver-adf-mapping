import copy
import csv
import json
import os
import pprint
import time
from collections import defaultdict
from dataclasses import dataclass, field

from anytree import Node, Resolver
from anytree.exporter import MermaidExporter

complete_pipelines: dict = {}  # key: pipeline_name, value: Node
incomplete_pipelines: dict = (
    {}
)  # key: pipeline_name, value: tuple of Node and list of dependent pipelines

all_stored_procedures: dict = {}  # key: stored_procedure_name, value: Node
all_views: dict = {}  # key: view_name, value: Node
all_tables: dict = {}  # key: table_name, value: Node
resolver = Resolver("name")

documents_dir = "C:\\Users\\chwalik.i\\Documents"

pipeline_dir = os.path.join(
    documents_dir, "da-ap-pda-napea-datafactory-backend", "pipeline"
)


@dataclass
class TableInView:
    ViewName: str
    Total: int = 0


@dataclass
class Pipeline:
    Name: str


@dataclass
class ObjectInStoredProcedure:
    Name: str
    Pipelines: list[Pipeline] = field(default_factory=list)
    Total: int = 0


@dataclass
class Table:
    Name: str
    Views: list[TableInView] = field(default_factory=list)
    StoredProcedures: list[ObjectInStoredProcedure] = field(default_factory=list)
    Pipelines: list[Pipeline] = field(default_factory=list)
    TotalReferences: int = 0


@dataclass
class View:
    Name: str
    StoredProcedures: list[ObjectInStoredProcedure] = field(default_factory=list)
    TotalReferences: int = 0


def countReferences() -> tuple[list[Table], list[View], list[ObjectInStoredProcedure]]:
    tables: list[Table] = []
    path_prefix = os.path.join("data", "sqldb-cdh-na-sl-dev-01")
    with open(os.path.join(path_prefix, "Tables.csv"), "r") as tables_file:
        for line in tables_file:
            table_name = line.strip()
            tables.append(Table(table_name))

    views: list[View] = []
    with open(os.path.join(path_prefix, "Views.csv"), "r") as views_file:
        reader = csv.reader(views_file, delimiter="	")
        for row in reader:
            view_name: str = row[0].lower()
            views.append(View(view_name))
            # check for Table reference
            table_root = Node("Tables")
            definition = row[1].lower()
            for table in tables:
                if table.Name.lower() in definition:
                    references_in_def = definition.count(table.Name)
                    table.TotalReferences += references_in_def
                    table.Views.append(TableInView(row[0], references_in_def))

                    table_root.children += (Node(table.Name),)

            view_node = Node(view_name, children=(table_root,))
            all_views[view_name] = view_node

    stored_procedures: list[ObjectInStoredProcedure] = []
    with open(os.path.join(path_prefix, "StoredProcedures.csv"), "r") as sp_file:
        reader = csv.reader(sp_file, delimiter="	")
        for row in reader:
            sp_name = row[0]
            sp_node = Node(sp_name)
            stored_procedures.append(ObjectInStoredProcedure(sp_name))

            definition = row[1].lower()
            table_root = Node("Tables")
            for table in tables:
                if table.Name.lower() in definition:
                    references_in_def = definition.count(table.Name)
                    table.TotalReferences += references_in_def
                    table.StoredProcedures.append(
                        ObjectInStoredProcedure(sp_name, references_in_def)
                    )

                    table_root.children += (Node(table.Name),)

            sp_node.children += (table_root,)

            view_root = Node("Views")
            for view in views:
                if view.Name.lower() in definition:
                    references_in_def = definition.count(view.Name)
                    view.TotalReferences += references_in_def
                    view.StoredProcedures.append(
                        ObjectInStoredProcedure(sp_name, references_in_def)
                    )
                    view_node = copy.deepcopy(all_views[view.Name])
                    view_root.children += (view_node,)

            sp_node.children += (view_root,)

            all_stored_procedures[sp_name] = sp_node

    return tables, views, stored_procedures


def createReport(tables: list[Table], views: list[View]):
    tables.sort(key=lambda x: x.TotalReferences, reverse=True)
    views.sort(key=lambda x: x.TotalReferences, reverse=True)

    with open("reports/table-report.txt", "w") as report_file:
        for table in tables:
            report_file.write(f"Table: {table.Name}\n")
            report_file.write(f"Total references: {table.TotalReferences}\n")
            report_file.write("Views:\n")
            for view in table.Views:
                report_file.write(f"\t{view.ViewName}: {view.Total}\n")
            report_file.write("Stored Procedures:\n")
            for sp in table.StoredProcedures:
                report_file.write(f"\t{sp.Name}: {sp.Total}\n")
            report_file.write("\n\n")

    with open("reports/view-report.txt", "w") as view_report_file:
        for view in views:
            view_report_file.write(f"View: {view.Name}\n")
            view_report_file.write(f"Total references: {view.TotalReferences}\n")
            view_report_file.write("Stored Procedures:\n")
            for sp in view.StoredProcedures:
                view_report_file.write(f"\t{sp.Name}: {sp.Total}\n")
            view_report_file.write("\n\n")


def bottomUpAttachment(parent_name: str):
    if incomplete_pipelines.get(parent_name) is None:
        return  # they're in complete_pipelines
    node, children = incomplete_pipelines[parent_name]
    for child in children:
        if incomplete_pipelines.get(child) is None:
            complete_child: Node = copy.deepcopy(complete_pipelines.get(child))

            dp_root: Node = Resolver("name").get(node, "Dependent Pipelines")
            dp_root.children += (complete_child,)

            children.remove(child)
            if len(children) == 0:
                complete_pipelines[parent_name] = node
                del incomplete_pipelines[parent_name]
        else:
            bottomUpAttachment(child)


def analyzePipelines():
    # Build Tree
    visited_pipelines: list[str] = []
    piplines = os.listdir(pipeline_dir)
    for pipeline in piplines:
        with open(os.path.join(pipeline_dir, pipeline), "r") as pipeline_file:
            # Read the name and create the node
            pipeline_json = json.load(pipeline_file)
            pipeline_name = pipeline_json["name"]

            table_root = Node("Tables")
            sp_root = Node("Stored Procedures")
            dp_root = Node("Dependent Pipelines")

            bad_table_root = Node("Tables")
            bad_sp_root = Node("Stored Procedures")
            bad_dp_root = Node("Dependent Pipelines")

            # check for Stored Procedures, Table lookups, and dependent pipelines
            activities: list = pipeline_json["properties"]["activities"]
            dependent_pipelines: list = []
            for activity in activities:
                match activity["type"]:
                    case "SqlServerStoredProcedure":
                        parsed_sp_name = activity["typeProperties"][
                            "storedProcedureName"
                        ]
                        if (
                            type(parsed_sp_name) is str
                        ):  # there is a case where this is a Dict like in TPO_dimProductCanada where the dict is a value of @activity('Get metadata')
                            stored_procedure_name: str = (
                                activity["typeProperties"]["storedProcedureName"]
                                .replace("[", "")
                                .replace("]", "")
                            )
                            stored_procedure: Node | None = all_stored_procedures.get(
                                stored_procedure_name
                            )
                            if stored_procedure is None:
                                bad_sp_root.children += (Node(stored_procedure_name),)
                            else:
                                sp_node: Node = copy.deepcopy(stored_procedure)
                                sp_root.children += (sp_node,)
                    case "Lookup":
                        # Some tables are hardcoded
                        match activity["typeProperties"]["dataset"]["referenceName"]:
                            case "NAPEA_Metadata_Table":
                                table_name = "mtdta.INPUT_SRC_MTDTA"
                            case "Backend_Dynamic_Table":
                                table_name: str = (
                                    activity["typeProperties"]["dataset"]["parameters"][
                                        "TableName"
                                    ]
                                    .replace("[", "")
                                    .replace("]", "")
                                )
                        table: Node | None = all_tables.get(table_name)
                        if table is None:
                            bad_table_root.children += (Node(table_name),)
                        else:
                            table_node: Node = copy.deepcopy(table)
                            table_root.children += (table_node,)
                    case "ExecutePipeline":  # the pipeline runs another pipeline
                        dependent_pipeline_name: str = activity["typeProperties"][
                            "pipeline"
                        ]["referenceName"]
                        dependent_pipelines.append(dependent_pipeline_name)

            bad_root = Node("Nonexistent")
            bad_root.children += (bad_table_root, bad_sp_root, bad_dp_root)

            pipeline_node = Node(pipeline_name)
            pipeline_node.children += (table_root, sp_root, dp_root, bad_root)

            has_dependent_pipeline = len(dependent_pipelines) > 0
            if has_dependent_pipeline:
                incomplete_pipelines[pipeline_name] = (
                    pipeline_node,
                    dependent_pipelines,
                )
            else:
                complete_pipelines[pipeline_name] = pipeline_node
            visited_pipelines.append(pipeline_name)
    # Attach dependent pipelines
    for pipeline_name in visited_pipelines:
        bottomUpAttachment(pipeline_name)


def createImages():
    for pipeline_name, pipeline_node in complete_pipelines.items():
        MermaidExporter(pipeline_node).to_file(
            os.path.join("images", f"{pipeline_name}.txt")
        )


def main():
    start_time = time.time()
    print("EXECUTING")
    table_result, view_result, sp_result = countReferences()

    # with open(os.path.join("debug", "raw-table-result.txt"), "w") as result_file:
    #     pprint.pp(table_result, result_file)
    # with open(os.path.join("debug", "raw-view-result.txt"), "w") as result_file:
    #     pprint.pp(view_result, result_file)

    analyzePipelines()

    # createReport(table_result, view_result)
    createImages()
    print("DONE")
    elapsed_time = time.time() - start_time
    print("Execution time:", elapsed_time, "\n")


main()
