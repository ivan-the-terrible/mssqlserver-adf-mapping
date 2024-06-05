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
from anytree.exporter import JsonExporter, MermaidExporter
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
    TotalReferences: int = 0


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


complete_pipelines: dict[str, Node] = {}  # key: pipeline_name, value: Node
incomplete_pipelines: dict[str, (Node, [])] = (
    {}
)  # key: pipeline_name, value: tuple of Node and list of dependent pipelines

all_stored_procedures: dict[str, Node] = {}  # key: stored_procedure_name, value: Node
all_views: dict[str, Node] = {}  # key: view_name, value: Node
all_tables: dict[str, Node] = {}  # key: table_name, value: Node

table_report: dict[str, Table] = {}  # key: table_name, value: Table
view_report: dict[str, View] = {}  # key: view_name, value: View
sp_report: dict[str, StoredProcedure] = (
    {}
)  # key: stored_procedure_name, value: StoredProcedure
pipeline_report: dict[str, Pipeline] = {}  # key: pipeline_name, value: Pipeline


resolver = Resolver("name")
debug = False


def checkDirectory(dir_path: str) -> str:
    if not os.path.exists(dir_path) or not os.path.isdir(dir_path):
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

    global table_report, view_report, sp_report

    with open(os.path.join(path_prefix, "Tables.csv"), "r") as tables_file:
        for line in tables_file:
            table_name = line.strip()
            # reporting
            table_report[table_name] = Table(table_name)
            # tree
            all_tables[table_name] = Node(table_name)

    with open(os.path.join(path_prefix, "Views.csv"), "r") as views_file:
        reader = csv.reader(views_file, delimiter="	")
        for row in reader:
            view_name: str = row[0].lower()
            view_report[view_name] = View(view_name)
            # check for Table reference
            table_root = Node("Tables")
            definition = row[1].lower().replace("[", "").replace("]", "")
            for table_name in table_report.keys():
                lowercase_table_name = table_name.lower()
                if lowercase_table_name in definition:
                    # reporting
                    references_in_def = definition.count(lowercase_table_name)
                    table_report[table_name].TotalReferences += references_in_def
                    table_report[table_name].TableInViews.append(
                        ObjectInView(view_name, references_in_def)
                    )
                    # tree
                    table_root.children += (Node(table_name),)

            view_node = Node(view_name, children=(table_root,))
            all_views[view_name] = view_node

    with open(os.path.join(path_prefix, "StoredProcedures.csv"), "r") as sp_file:
        reader = csv.reader(sp_file, delimiter="	")
        for row in reader:
            sp_name = row[0]
            sp_node = Node(sp_name)
            sp_report[sp_name] = StoredProcedure(sp_name)

            definition = row[1].lower().replace("[", "").replace("]", "")
            table_root = Node("Tables")
            for table_name in table_report.keys():
                lowercase_table_name = table_name.lower()
                if lowercase_table_name in definition:
                    references_in_def = definition.count(lowercase_table_name)
                    table_report[table_name].TotalReferences += references_in_def
                    table_report[table_name].TableInStoredProcedures.append(
                        ObjectInStoredProcedure(sp_name, references_in_def)
                    )

                    table_root.children += (Node(table_name),)

            sp_node.children += (table_root,)

            view_root = Node("Views")
            for view_name in view_report.keys():
                lowercase_view_name = view_name.lower()
                if lowercase_view_name in definition:
                    references_in_def = definition.count(lowercase_view_name)
                    view_report[view_name].TotalReferences += references_in_def
                    view_report[view_name].ViewInStoredProcedures.append(
                        ObjectInStoredProcedure(sp_name, references_in_def)
                    )
                    view_node = copy.deepcopy(all_views[view_name])
                    view_root.children += (view_node,)

            sp_node.children += (view_root,)

            all_stored_procedures[sp_name] = sp_node
    print("Tables, Views, Stored Procedure References counted")


def createTablesReport(output_dir: str):
    print("Creating table report")
    sorted_tables: list[Table] = list(table_report.values())
    sorted_tables.sort(key=lambda x: x.TotalReferences, reverse=True)
    with open(os.path.join(output_dir, "table-report.txt"), "w") as report_file:
        for table in sorted_tables:
            report_file.write(f"Table: {table.Name}\n")
            report_file.write(f"Total references: {table.TotalReferences}\n")
            report_file.write("Views:\n")
            for view in table.TableInViews:
                report_file.write(f"\t{view.ViewName}: {view.Total}\n")
            report_file.write("Stored Procedures:\n")
            for sp in table.TableInStoredProcedures:
                report_file.write(f"\t{sp.StoredProcedureName}: {sp.Total}\n")
            report_file.write("Pipelines:\n")
            for pipeline in table.TableInPipelines:
                report_file.write(f"\t{pipeline.PipelineName}: {pipeline.Total}\n")
            report_file.write("\n\n")
    print("Table report created")


def createViewsReport(output_dir: str):
    print("Creating view report")
    sorted_views: list[View] = list(view_report.values())
    sorted_views.sort(key=lambda x: x.TotalReferences, reverse=True)
    with open(os.path.join(output_dir, "view-report.txt"), "w") as report_file:
        for view in sorted_views:
            report_file.write(f"View: {view.Name}\n")
            report_file.write(f"Total references: {view.TotalReferences}\n")
            report_file.write("Stored Procedures:\n")
            for sp in view.ViewInStoredProcedures:
                report_file.write(f"\t{sp.StoredProcedureName}: {sp.Total}\n")
            report_file.write("\n\n")
    print("View report created")


def createStoredProceduresReport(output_dir: str):
    print("Creating stored procedures report")
    sorted_sp: list[StoredProcedure] = list(sp_report.values())
    sorted_sp.sort(key=lambda x: x.TotalReferences, reverse=True)
    with open(
        os.path.join(output_dir, "stored-procedures-report.txt"), "w"
    ) as report_file:
        for sp in sorted_sp:
            report_file.write(f"Stored Procedure: {sp.Name}\n")
            report_file.write(f"Total references: {sp.TotalReferences}\n")
            report_file.write("Pipelines:\n")
            for pipeline in sp.StoredProcedureInPipelines:
                report_file.write(f"\t{pipeline.PipelineName}: {pipeline.Total}\n")
            report_file.write("\n\n")
    print("Stored procedures report created")


def createPipelinesReport(output_dir: str):
    print("Creating pipelines report")
    sorted_pipelines: list[Pipeline] = list(pipeline_report.values())
    sorted_pipelines.sort(key=lambda x: x.Total, reverse=True)
    with open(os.path.join(output_dir, "pipeline-report.txt"), "w") as report_file:
        for pipeline in sorted_pipelines:
            report_file.write(f"Pipeline: {pipeline.Name}\n")
            report_file.write(f"Total references: {pipeline.Total}\n")
            report_file.write("Dependent Pipelines:\n")
            for pipeline in pipeline.PipelineInPipelines:
                report_file.write(f"\t{pipeline.PipelineName}: {pipeline.Total}\n")
            report_file.write("\n\n")
    print("Pipelines report created")


def createReport():
    output_dir_name = os.getenv("OUTPUT_DIR")
    output_dir = os.path.join("reports", output_dir_name)
    os.makedirs(output_dir, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(createTablesReport, output_dir),
            executor.submit(createViewsReport, output_dir),
            executor.submit(createStoredProceduresReport, output_dir),
            executor.submit(createPipelinesReport, output_dir),
        ]
        concurrent.futures.wait(futures)


def bottomUpAttachment(parent_name: str):
    if parent_name not in incomplete_pipelines:
        return complete_pipelines[parent_name]
    node, children = incomplete_pipelines[parent_name]
    copy_children = children.copy()
    for child in children:
        complete_child = ""
        if child not in incomplete_pipelines:
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


def process_activities(
    activities: list,
    table_root: Node,
    sp_root: Node,
    dp_root: Node,
    bad_table_root: Node,
    bad_sp_root: Node,
    bad_dp_root: Node,
    dependent_pipelines: list,
    total_references: dict[str, dict[str, int]],
):
    for activity in activities:
        match activity["type"]:
            case "SqlServerStoredProcedure":
                parsed_sp_name = activity["typeProperties"]["storedProcedureName"]
                if isinstance(parsed_sp_name, str):
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
                        # tree
                        sp_node: Node = copy.deepcopy(stored_procedure)
                        sp_root.children += (sp_node,)
                        # reporting
                        ref_count = total_references["sp"].get(stored_procedure_name)
                        ref_count = 1 if ref_count is None else ref_count + 1
                        total_references["sp"][stored_procedure_name] = ref_count

            case "Lookup":
                # sqlReaderQuery can sometimes be a dict or a string
                if "AzureSqlSource" in activity["typeProperties"]["source"]["type"]:
                    sqlReaderQuery_prop = activity["typeProperties"]["source"][
                        "sqlReaderQuery"
                    ]
                    if isinstance(sqlReaderQuery_prop, str):
                        definition = sqlReaderQuery_prop
                    else:
                        definition: str | None = sqlReaderQuery_prop["value"]
                    if definition is not None:
                        query = definition.replace("[", "").replace("]", "").lower()

                        for table in table_report.keys():
                            lowercase_table_name = table.lower()
                            if lowercase_table_name in query:
                                # reporting
                                ref_count = total_references["table"].get(table)
                                ref_count = 1 if ref_count is None else ref_count + 1
                                total_references["table"][table] = ref_count
                                # tree
                                table_node: Node = copy.deepcopy(all_tables[table])
                                table_root.children += (table_node,)

            case "ExecutePipeline":  # the pipeline runs another pipeline
                dependent_pipeline_name: str = activity["typeProperties"]["pipeline"][
                    "referenceName"
                ]
                dependent_pipelines.append(dependent_pipeline_name)
                # reporting
                ref_count = total_references["dp"].get(dependent_pipeline_name)
                ref_count = 1 if ref_count is None else ref_count + 1
                total_references["dp"][dependent_pipeline_name] = ref_count

            case "IfCondition":
                conditional_activities = []
                true_activities = activity["typeProperties"].get("ifTrueActivities", [])
                false_activities = activity["typeProperties"].get(
                    "ifFalseActivities", []
                )
                conditional_activities += true_activities
                conditional_activities += false_activities
                if len(conditional_activities) > 0:
                    process_activities(
                        conditional_activities,
                        table_root,
                        sp_root,
                        dp_root,
                        bad_table_root,
                        bad_sp_root,
                        bad_dp_root,
                        dependent_pipelines,
                        total_references,
                    )


def analyzePipelines():
    pipeline_dir = checkEnvironmentVariable("PIPELINE_DIR")
    # Build Tree
    global pipeline_report, table_report, sp_report
    need_to_complete_pipelines: list[str] = []
    pipelines = os.listdir(pipeline_dir)
    for pipeline in pipelines:
        with open(os.path.join(pipeline_dir, pipeline), "r") as pipeline_file:
            # Read the name and create the node
            pipeline_json = json.load(pipeline_file)
            pipeline_name = pipeline_json["name"]

            pipeline_report[pipeline_name] = Pipeline(pipeline_name)

            table_root = Node("Tables")
            sp_root = Node("Stored Procedures")
            dp_root = Node("Dependent Pipelines")

            bad_table_root = Node("Tables")
            bad_sp_root = Node("Stored Procedures")
            bad_dp_root = Node("Dependent Pipelines")

            # check for Stored Procedures, Table lookups, and dependent pipelines
            activities: list = pipeline_json["properties"]["activities"]
            dependent_pipelines: list = []

            # reporting counts
            total_references: dict[str, dict[str, int]] = {
                "table": {},
                "sp": {},
                "dp": {},
            }  # keep a list of tables/stored procedures/pipelines that are referenced in the pipeline
            # we need to do this since a pipeline could reference the same activity multiple times
            # ex. {sp: {sp_name: 2, sp_name2: 1}, table: {table_name: 1}, dp: {dp_name: 1}

            # loop through Activities
            process_activities(
                activities,
                table_root,
                sp_root,
                dp_root,
                bad_table_root,
                bad_sp_root,
                bad_dp_root,
                dependent_pipelines,
                total_references,
            )
            # reporting
            for ref_type, ref_dict in total_references.items():
                for ref_name, ref_count in ref_dict.items():
                    if ref_type == "sp":
                        sp_report[ref_name].TotalReferences += ref_count
                        sp_report[ref_name].StoredProcedureInPipelines.append(
                            ObjectInPipeline(pipeline_name, ref_count)
                        )
                    elif ref_type == "table":
                        table_report[ref_name].TotalReferences += ref_count
                        table_report[ref_name].TableInPipelines.append(
                            ObjectInPipeline(pipeline_name, ref_count)
                        )
                    elif ref_type == "dp":
                        pipeline_report[pipeline_name].Total += ref_count
                        pipeline_report[pipeline_name].PipelineInPipelines.append(
                            ObjectInPipeline(ref_name, ref_count)
                        )
            # tree
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


def saveJSON(exporter: JsonExporter, node: Node, filename: str):
    print("Saving JSON to ", filename)
    with open(filename, "w") as file:
        exporter.write(node, file)


def exportImagesAndTreeStructures():
    # Ensure MermaidJS is installed
    has_mermaidJS = os.system("mmdc --version") == 0  # check exit status
    if not has_mermaidJS:
        print("MermaidJS not installed or mmdc command not callable. Exiting...")
        return

    output_dir_name = os.getenv("OUTPUT_DIR")
    output_dir = os.path.join("images", output_dir_name)

    os.makedirs(os.path.join(output_dir, "mermaid"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "pdf"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "json"), exist_ok=True)

    json_exporter = JsonExporter(indent=2)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for pipeline_name, pipeline_node in complete_pipelines.items():
            mermaid = MermaidExporter(pipeline_node)
            if debug:
                pipeline_mermaid_file = os.path.join(
                    output_dir, "mermaid", f"{pipeline_name}.mmd"
                )
                futures.append(mermaid.to_file, pipeline_mermaid_file)

            # call MermaidJS to generate the diagram
            # pipeline_svg = os.path.join("images", "svg", f"{pipeline_name}.svg")
            pipeline_pdf = os.path.join(output_dir, "pdf", f"{pipeline_name}.pdf")
            mermaid_text = "\n".join(mermaid)
            future_pdf = executor.submit(
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
            futures.append(future_pdf)

            pipeline_json_file = os.path.join(
                output_dir, "json", f"{pipeline_name}.json"
            )
            future_json = executor.submit(
                saveJSON, json_exporter, pipeline_node, pipeline_json_file
            )
            futures.append(future_json)

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

    createReport()
    exportImagesAndTreeStructures()
    print("DONE")
    elapsed_time = time.time() - start_time
    print("Execution time:", elapsed_time, "\n")


main()
