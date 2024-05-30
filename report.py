import codecs
import concurrent.futures
import copy
import csv
import json
import os
import pprint
import subprocess
import time
from dataclasses import dataclass, field

from anytree import Node, Resolver
from anytree.exporter import MermaidExporter
from dotenv import load_dotenv


class MermaidExporter(MermaidExporter):
    """
    I needed to overwrite this class because I don't want the header and footer on the file.
    Original to_file method would file.write("```mermaid\n")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_file(self, filename):
        with codecs.open(filename, "w", "utf-8") as file:
            for line in self:
                file.write("%s\n" % line)


@dataclass
class ObjectInView:
    ViewName: str
    Total: int = 0


@dataclass
class ObjectInStoredProcedure:
    StoredProcedureName: str
    Total: int = 0


@dataclass
class ObjectInPipeline:
    PipelineName: str
    Total: int = 0


@dataclass
class Pipeline:
    Name: str
    PipelineInPipelines: list[ObjectInPipeline] = field(default_factory=list)
    Total: int = 0


@dataclass
class StoredProcedure:
    Name: str
    StoredProcedureInPipelines: list[ObjectInPipeline] = field(default_factory=list)
    Total: int = 0


@dataclass
class Table:
    Name: str
    TableInViews: list[ObjectInView] = field(default_factory=list)
    TableInStoredProcedures: list[ObjectInStoredProcedure] = field(default_factory=list)
    TableInPipelines: list[ObjectInPipeline] = field(default_factory=list)
    TotalReferences: int = 0


@dataclass
class View:
    Name: str
    ViewInStoredProcedures: list[ObjectInStoredProcedure] = field(default_factory=list)
    TotalReferences: int = 0


complete_pipelines: dict = {}  # key: pipeline_name, value: Node
incomplete_pipelines: dict = (
    {}
)  # key: pipeline_name, value: tuple of Node and list of dependent pipelines

all_stored_procedures: dict = {}  # key: stored_procedure_name, value: Node
all_views: dict = {}  # key: view_name, value: Node
all_tables: dict = {}  # key: table_name, value: Node

table_report: list[Table] = []
view_report: list[View] = []
sp_report: list[StoredProcedure] = []


resolver = Resolver("name")
debug = False


def checkDirectory(dir_path: str) -> str:
    if not os.path.exists(dir_path) | os.path.isdir(dir_path):
        print(f"{dir_path} does not exist or isn't directory")
        exit(1)
    return dir_path


def checkEnvironmentVariable(env_var: str) -> str:
    dir_path = os.getenv(env_var)
    if dir_path is None:
        print(f"{env_var} not set")
        exit(1)
    return checkDirectory(dir_path)


def countReferences():
    mssqlserver_dir = os.getenv("MSSQL_SERVER_DATA_DIR")
    path_prefix = checkDirectory(os.path.join("data", mssqlserver_dir))

    tables: list[Table] = []
    with open(os.path.join(path_prefix, "Tables.csv"), "r") as tables_file:
        for line in tables_file:
            table_name = line.strip()
            tables.append(Table(table_name))

            all_tables[table_name] = Node(table_name)

    views: list[View] = []
    with open(os.path.join(path_prefix, "Views.csv"), "r") as views_file:
        reader = csv.reader(views_file, delimiter="	")
        for row in reader:
            view_name: str = row[0].lower()
            views.append(View(view_name))
            # check for Table reference
            table_root = Node("Tables")
            definition = row[1].lower().replace("[", "").replace("]", "")
            for table in tables:
                if table.Name.lower() in definition:
                    references_in_def = definition.count(table.Name)
                    table.TotalReferences += references_in_def
                    table.TableInViews.append(ObjectInView(row[0], references_in_def))

                    table_root.children += (Node(table.Name),)

            view_node = Node(view_name, children=(table_root,))
            all_views[view_name] = view_node

    stored_procedures: list[StoredProcedure] = []
    with open(os.path.join(path_prefix, "StoredProcedures.csv"), "r") as sp_file:
        reader = csv.reader(sp_file, delimiter="	")
        for row in reader:
            sp_name = row[0]
            sp_node = Node(sp_name)
            stored_procedures.append(StoredProcedure(sp_name))

            definition = row[1].lower().replace("[", "").replace("]", "")
            table_root = Node("Tables")
            for table in tables:
                if table.Name.lower() in definition:
                    references_in_def = definition.count(table.Name)
                    table.TotalReferences += references_in_def
                    table.TableInStoredProcedures.append(
                        StoredProcedure(sp_name, references_in_def)
                    )

                    table_root.children += (Node(table.Name),)

            sp_node.children += (table_root,)

            view_root = Node("Views")
            for view in views:
                if view.Name.lower() in definition:
                    references_in_def = definition.count(view.Name)
                    view.TotalReferences += references_in_def
                    view.ViewInStoredProcedures.append(
                        StoredProcedure(sp_name, references_in_def)
                    )
                    view_node = copy.deepcopy(all_views[view.Name])
                    view_root.children += (view_node,)

            sp_node.children += (view_root,)

            all_stored_procedures[sp_name] = sp_node

    global table_report, view_report, sp_report
    table_report = tables
    view_report = views
    sp_report = stored_procedures


def createReport(tables: list[Table], views: list[View]):
    output_dir_name = os.getenv("OUTPUT_DIR")
    output_dir = os.path.join("reports", output_dir_name)
    os.makedirs(output_dir, exist_ok=True)

    tables.sort(key=lambda x: x.TotalReferences, reverse=True)
    views.sort(key=lambda x: x.TotalReferences, reverse=True)

    with open(os.path.join(output_dir, "table-report.txt"), "w") as report_file:
        for table in tables:
            report_file.write(f"Table: {table.Name}\n")
            report_file.write(f"Total references: {table.TotalReferences}\n")
            report_file.write("Views:\n")
            for view in table.TableInViews:
                report_file.write(f"\t{view.ViewName}: {view.Total}\n")
            report_file.write("Stored Procedures:\n")
            for sp in table.TableInStoredProcedures:
                report_file.write(f"\t{sp.Name}: {sp.Total}\n")
            report_file.write("\n\n")

    with open(os.path.join(output_dir, "view-report.txt"), "w") as view_report_file:
        for view in views:
            view_report_file.write(f"View: {view.Name}\n")
            view_report_file.write(f"Total references: {view.TotalReferences}\n")
            view_report_file.write("Stored Procedures:\n")
            for sp in view.ViewInStoredProcedures:
                view_report_file.write(f"\t{sp.Name}: {sp.Total}\n")
            view_report_file.write("\n\n")


def bottomUpAttachment(parent_name: str):
    if incomplete_pipelines.get(parent_name) is None:
        return complete_pipelines[parent_name]
    node, children = incomplete_pipelines[parent_name]
    copy_children = children.copy()
    for child in children:
        complete_child = ""
        if incomplete_pipelines.get(child) is None:
            complete_child = copy.deepcopy(complete_pipelines.get(child))
        else:
            complete_child = copy.deepcopy(bottomUpAttachment(child))

        dp_root: Node = Resolver("name").get(node, "Dependent Pipelines")
        dp_root.children += (complete_child,)

        copy_children.remove(child)
        if len(copy_children) == 0:
            complete_pipelines[parent_name] = node
            del incomplete_pipelines[parent_name]
            return complete_pipelines[parent_name]


def analyzePipelines():
    pipeline_dir = checkEnvironmentVariable("PIPELINE_DIR")
    # Build Tree
    need_to_complete_pipelines: list[str] = []
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
                need_to_complete_pipelines.append(pipeline_name)
            else:
                complete_pipelines[pipeline_name] = pipeline_node
    # Attach dependent pipelines
    for pipeline_name in need_to_complete_pipelines:
        bottomUpAttachment(pipeline_name)


def createImages():
    # Ensure MermaidJS is installed
    has_mermaidJS = os.system("mmdc --version") == 0  # check exit status
    if not has_mermaidJS:
        print("MermaidJS not installed or mmdc command not callable. Exiting...")
        return

    output_dir_name = os.getenv("OUTPUT_DIR")
    output_dir = os.path.join("images", output_dir_name)

    os.makedirs(os.path.join(output_dir, "mermaid"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "pdf"), exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for pipeline_name, pipeline_node in complete_pipelines.items():
            mermaid = MermaidExporter(pipeline_node)
            if debug:
                mermaid.to_file(
                    os.path.join(output_dir, "mermaid", f"{pipeline_name}.mmd")
                )

            # call MermaidJS to generate the diagram
            # pipeline_svg = os.path.join("images", "svg", f"{pipeline_name}.svg")
            pipeline_pdf = os.path.join(output_dir, "pdf", f"{pipeline_name}.pdf")
            mermaid_text = "\n".join(mermaid)
            future = executor.submit(
                subprocess.run,
                [
                    "mmdc",
                    "--input",
                    "-",
                    "--output",
                    pipeline_pdf,
                    "--configFile",
                    "mermaid-config.json",
                ],
                input=mermaid_text.encode(),
                shell=True,
            )
            futures.append(future)

        # Wait for all tasks to complete
        concurrent.futures.wait(futures)


def main():
    start_time = time.time()
    print("EXECUTING")

    load_dotenv()
    global debug
    debug = os.getenv("DEBUG") == "True"

    countReferences()
    if debug:
        os.makedirs("debug", exist_ok=True)
        with open(os.path.join("debug", "raw-table-result.txt"), "w") as result_file:
            pprint.pp(table_report, result_file)
        with open(os.path.join("debug", "raw-view-result.txt"), "w") as result_file:
            pprint.pp(view_report, result_file)

    analyzePipelines()

    createReport(table_report, view_report)
    # createImages()
    print("DONE")
    elapsed_time = time.time() - start_time
    print("Execution time:", elapsed_time, "\n")


main()
