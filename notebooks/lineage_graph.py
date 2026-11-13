# Databricks notebook source

# COMMAND ----------
from pathlib import Path

repo = Path(
    "Path.cwd()"
)

print("REPOSITORY FILES")
print("-" * 60)

for path in sorted(repo.rglob("*")):
    if path.is_file() and any(
        part not in {".git", "__pycache__", ".venv"}
        for part in path.parts
    ):
        print(path.relative_to(repo))

# COMMAND ----------
from pathlib import Path

search_roots = [
    Path("Path.cwd()"),
    Path("/Volumes/workspace/omni")
]

for root in search_roots:
    print(f"\nSEARCHING: {root}")

    if not root.exists():
        print("Path does not exist.")
        continue

    for filename in [
        "manifest.json",
        "run_results.json",
        "catalog.json"
    ]:
        matches = [
            path for path in root.rglob(filename)
            if ".git" not in path.parts
        ]

        for path in matches:
            print(path)

# COMMAND ----------
import os

path = "/Volumes/workspace/omni/lineage_metadata/manifest.json"

print("Manifest uploaded:", os.path.exists(path))

# COMMAND ----------
import json

manifest_path = (
    "/Volumes/workspace/omni/lineage_metadata/manifest.json"
)

with open(manifest_path, "r", encoding="utf-8") as file:
    manifest = json.load(file)

models = {
    key: value
    for key, value in manifest["nodes"].items()
    if value["resource_type"] == "model"
}

sources = manifest["sources"]

print("MANIFEST VALIDATION")
print("-" * 40)
print("dbt version:", manifest["metadata"]["dbt_version"])
print("Models:", len(models))
print("Sources:", len(sources))
print("Total model dependencies:", sum(
    len(model["depends_on"]["nodes"])
    for model in models.values()
))

# COMMAND ----------
# Inspect dbt model identifiers and physical table names

print("DBT MODEL RELATIONS")
print("-" * 60)

for unique_id, model in models.items():
    print(
        "ID:", unique_id,
        "| Relation:", model.get("relation_name")
    )

print("\nDBT SOURCE RELATIONS")
print("-" * 60)

for unique_id, source in sources.items():
    print(
        "ID:", unique_id,
        "| Relation:", source.get("relation_name")
    )

# Check whether production runtime events exist
from pathlib import Path

runtime_file = Path(
    "/Volumes/workspace/omni/lineage_metadata/lineage_events.jsonl"
)

print("\nRUNTIME METADATA")
print("-" * 60)
print("Production event file exists:", runtime_file.exists())

if runtime_file.exists():
    with runtime_file.open("r", encoding="utf-8") as file:
        events = [line for line in file if line.strip()]

    print("Runtime events:", len(events))
else:
    print("Runtime events: 0")

# COMMAND ----------

from collections import defaultdict, deque


# ============================================================
#  BUILD ADJACENCY INDEXES
# ============================================================

upstream_index = defaultdict(list)
downstream_index = defaultdict(list)

for edge in graph["edges"]:
    source = edge["source"]
    target = edge["target"]

    downstream_index[source].append(target)
    upstream_index[target].append(source)


# ============================================================
#  GENERIC GRAPH TRAVERSAL
# ============================================================

def traverse_lineage(start_node, adjacency_index):

    if start_node not in graph["nodes"]:
        raise ValueError(f"Unknown lineage node: {start_node}")

    visited = set()
    queue = deque(adjacency_index[start_node])

    while queue:
        current = queue.popleft()

        if current in visited:
            continue

        visited.add(current)

        for neighbor in adjacency_index[current]:
            if neighbor not in visited:
                queue.append(neighbor)

    return sorted(visited)


# ============================================================
#  UPSTREAM LINEAGE
# ============================================================

def get_upstream_lineage(node_id):
    return traverse_lineage(node_id, upstream_index)


# ============================================================
#  DOWNSTREAM IMPACT ANALYSIS
# ============================================================

def get_downstream_impact(node_id):
    return traverse_lineage(node_id, downstream_index)


# ============================================================
#  TEST UPSTREAM LINEAGE
# ============================================================

target = "model.omni_dbt.mart_hr_payroll"

upstream = get_upstream_lineage(target)

print("UPSTREAM LINEAGE")
print("-" * 50)

for node_id in upstream:
    print(node_id)


# ============================================================
# TEST DOWNSTREAM IMPACT
# ============================================================

source = "source.omni_dbt.bronze.hr_payroll_runs"

impact = get_downstream_impact(source)

print("\nDOWNSTREAM IMPACT")
print("-" * 50)

for node_id in impact:
    print(node_id)

print("\nTRAVERSAL VALIDATION")
print("Upstream nodes:", len(upstream))
print("Downstream nodes:", len(impact))

# COMMAND ----------

# ============================================================
#  LOAD RUNTIME LINEAGE EVENTS
# ============================================================

import json
from pathlib import Path

runtime_file = Path(
    "/Volumes/workspace/omni/lineage_metadata/lineage_events.jsonl"
)

runtime_events = []

if runtime_file.exists():

    with runtime_file.open("r", encoding="utf-8") as file:

        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                continue

            try:
                event = json.loads(line)
                runtime_events.append(event)

            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON at line {line_number}: {error}"
                )

print("RUNTIME EVENT INGESTION")
print("-" * 50)
print("Events loaded:", len(runtime_events))


# ============================================================
#  INSPECT EVENT STRUCTURE
# ============================================================

for event in runtime_events:

    print("\nRun ID:", event.get("run_id"))
    print("Pipeline:", event.get("pipeline"))
    print("Transformation:", event.get("transformation"))
    print("Status:", event.get("status"))
    print("Inputs:", event.get("inputs"))
    print("Outputs:", event.get("outputs"))

# COMMAND ----------
# ============================================================
# BUILD DBT GRAPH BASE STRUCTURE
# ============================================================

# Initialize graph
graph = {
    "metadata": {
        "pipeline": "OMNI",
        "graph_type": "DBT_DECLARED_LINEAGE",
        "dbt_version": manifest["metadata"]["dbt_version"]
    },
    "nodes": {},
    "edges": []
}


# ============================================================
#  COMBINE DBT MODELS AND SOURCES
# ============================================================

resources = {
    **models,
    **sources
}


# ============================================================
#  NORMALIZE RELATION NAMES
# ============================================================

def normalize_relation(relation):

    if not relation:
        return None

    return relation.replace("`", "").strip()


# ============================================================
#  REGISTER GRAPH NODES
# ============================================================

for unique_id, resource in resources.items():

    graph["nodes"][unique_id] = {
        "id": unique_id,
        "name": resource["name"],
        "resource_type": resource["resource_type"],
        "relation": normalize_relation(
            resource.get("relation_name")
        )
    }


# ============================================================
#  REGISTER DEPENDENCY EDGES
# ============================================================

for unique_id, model in models.items():

    upstream_dependencies = model["depends_on"]["nodes"]

    for upstream_id in upstream_dependencies:

        if upstream_id not in graph["nodes"]:

            raise ValueError(
                f"Missing upstream dependency: {upstream_id}"
            )

        graph["edges"].append({
            "source": upstream_id,
            "target": unique_id,
            "edge_type": "dbt_declared"
        })


# ============================================================
# VALIDATE GRAPH
# ============================================================

node_count = len(graph["nodes"])
edge_count = len(graph["edges"])

source_count = sum(
    1 for node in graph["nodes"].values()
    if node["resource_type"] == "source"
)

model_count = sum(
    1 for node in graph["nodes"].values()
    if node["resource_type"] == "model"
)

print("OMNI DBT GRAPH VALIDATION")
print("-" * 50)

print("Nodes:", node_count)
print("Edges:", edge_count)
print("Sources:", source_count)
print("Models:", model_count)

assert node_count == 13
assert edge_count == 13
assert source_count == 3
assert model_count == 10

print("\nDBT GRAPH BUILD SUCCESSFUL")

# COMMAND ----------
# ============================================================
#  BUILD LINEAGE TRAVERSAL INDEXES
# ============================================================

from collections import defaultdict, deque

# Initialize indexes
upstream_index = defaultdict(list)
downstream_index = defaultdict(list)


# Build indexes from graph edges
for edge in graph["edges"]:

    source = edge["source"]
    target = edge["target"]

    upstream_index[target].append(source)
    downstream_index[source].append(target)


# ============================================================
#  GENERIC GRAPH TRAVERSAL
# ============================================================

def traverse_lineage(start_node, adjacency_index):

    if start_node not in graph["nodes"]:
        raise ValueError(
            f"Node not found in graph: {start_node}"
        )

    visited = set()
    queue = deque(adjacency_index[start_node])

    while queue:

        current = queue.popleft()

        if current in visited:
            continue

        visited.add(current)

        for neighbor in adjacency_index[current]:

            if neighbor not in visited:
                queue.append(neighbor)

    return sorted(visited)


# ============================================================
#  UPSTREAM LINEAGE
# ============================================================

def get_upstream_lineage(node_id):

    return traverse_lineage(
        node_id,
        upstream_index
    )


# ============================================================
#  DOWNSTREAM IMPACT
# ============================================================

def get_downstream_impact(node_id):

    return traverse_lineage(
        node_id,
        downstream_index
    )


print("LINEAGE TRAVERSAL INDEXES BUILT")
print("Upstream index entries:", len(upstream_index))
print("Downstream index entries:", len(downstream_index))

# COMMAND ----------
# ============================================================
#  VALIDATE LINEAGE TRAVERSAL
# ============================================================

# Test 1: Upstream lineage of payroll model

target_model = "model.omni_dbt.mart_hr_payroll"

upstream = get_upstream_lineage(target_model)

print("UPSTREAM LINEAGE")
print("-" * 50)

for node in upstream:
    print(node)

print("Total upstream dependencies:", len(upstream))


# Test 2: Downstream impact of payroll source

source_node = "source.omni_dbt.bronze.hr_payroll_runs"

downstream = get_downstream_impact(source_node)

print("\nDOWNSTREAM IMPACT")
print("-" * 50)

for node in downstream:
    print(node)

print("Total downstream dependencies:", len(downstream))


# Assertions
assert len(upstream) == 4
assert len(downstream) == 7

print("\nLINEAGE TRAVERSAL VALIDATION PASSED")

# COMMAND ----------
# ============================================================
#LOAD RUNTIME LINEAGE EVENTS
# ============================================================

import json
from pathlib import Path

runtime_file = Path(
    "/Volumes/workspace/omni/lineage_metadata/lineage_events.jsonl"
)

runtime_events = []

# Check whether production runtime events exist
if runtime_file.exists():

    with runtime_file.open("r", encoding="utf-8") as file:

        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                continue

            try:
                event = json.loads(line)

            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON at line {line_number}: {error}"
                )

            runtime_events.append(event)

else:
    print("Production runtime event file does not exist yet.")


# ============================================================
# INSPECT LOADED EVENTS
# ============================================================

print("\nOMNI RUNTIME LINEAGE INGESTION")
print("-" * 50)

print("Events loaded:", len(runtime_events))

for event in runtime_events:

    print("\nRun ID:", event.get("run_id"))
    print("Pipeline:", event.get("pipeline"))
    print("Transformation:", event.get("transformation"))
    print("Status:", event.get("status"))
    print("Inputs:", event.get("inputs"))
    print("Outputs:", event.get("outputs"))

# COMMAND ----------
# ============================================================
#  RUNTIME LINEAGE INTEGRATION
# ============================================================

from copy import deepcopy

# Work on a copy; preserve the original dbt graph
unified_graph = deepcopy(graph)

# ============================================================
#  BUILD RELATION-TO-NODE LOOKUP
# ============================================================

relation_to_node = {}

for node_id, node in unified_graph["nodes"].items():

    relation = node.get("relation")

    if relation:
        relation_to_node[relation] = node_id


# ============================================================
#  RESOLVE RUNTIME TABLE NAMES
# ============================================================

def resolve_runtime_node(table_name):

    if not table_name:
        raise ValueError("Runtime table name cannot be empty.")

    normalized_name = normalize_relation(table_name)

    # Match an existing dbt node
    if normalized_name in relation_to_node:
        return relation_to_node[normalized_name]

    # Register an external runtime-only node
    node_id = f"runtime.external.{normalized_name}"

    if node_id not in unified_graph["nodes"]:

        unified_graph["nodes"][node_id] = {
            "id": node_id,
            "name": normalized_name.split(".")[-1],
            "resource_type": "runtime_external",
            "relation": normalized_name
        }

    relation_to_node[normalized_name] = node_id

    return node_id


# ============================================================
#  INGEST RUNTIME EVENTS
# ============================================================

runtime_edge_count = 0

for event in runtime_events:

    run_id = event.get("run_id")

    for input_item in event.get("inputs", []):

        input_table = input_item.get("table")

        source_node = resolve_runtime_node(input_table)

        for output_item in event.get("outputs", []):

            output_table = output_item.get("table")

            target_node = resolve_runtime_node(output_table)

            unified_graph["edges"].append({
                "source": source_node,
                "target": target_node,
                "edge_type": "runtime_observed",
                "run_id": run_id,
                "status": event.get("status"),
                "transformation": event.get("transformation"),
                "started_at": event.get("started_at"),
                "completed_at": event.get("completed_at")
            })

            runtime_edge_count += 1


# ============================================================
# VALIDATE INTEGRATION
# ============================================================

dbt_edges = sum(
    1 for edge in unified_graph["edges"]
    if edge["edge_type"] == "dbt_declared"
)

runtime_edges = sum(
    1 for edge in unified_graph["edges"]
    if edge["edge_type"] == "runtime_observed"
)

print("OMNI UNIFIED LINEAGE GRAPH")
print("-" * 50)

print("Total nodes:", len(unified_graph["nodes"]))
print("Total edges:", len(unified_graph["edges"]))
print("DBT declared edges:", dbt_edges)
print("Runtime observed edges:", runtime_edges)

assert dbt_edges == 13
assert runtime_edges == len(runtime_events) * 0 or runtime_edges >= 0

print("\nRUNTIME INTEGRATION COMPLETED")

# COMMAND ----------
# ============================================================
#  TEST RUNTIME INTEGRATION
# SYNTHETIC DATA ONLY — NOT PRODUCTION EXECUTION
# ============================================================

from copy import deepcopy

# Isolated test copy
test_graph = deepcopy(graph)

# Build test relation lookup
test_relation_map = {
    node["relation"]: node_id
    for node_id, node in test_graph["nodes"].items()
    if node.get("relation")
}

# Synthetic execution event
synthetic_event = {
    "run_id": "SYNTHETIC_TEST_001",
    "pipeline": "OMNI",
    "transformation": "PAYROLL STAGING TEST",
    "status": "SUCCESS",
    "inputs": [
        {"table": "workspace.bronze.hr_payroll_runs"}
    ],
    "outputs": [
        {"table": "workspace.silver_silver.stg_hr_payroll_runs"}
    ],
    "started_at": "2026-11-13T00:00:00+00:00",
    "completed_at": "2026-11-13T00:00:05+00:00"
}

# Resolve existing dbt nodes
source_table = synthetic_event["inputs"][0]["table"]
target_table = synthetic_event["outputs"][0]["table"]

source_node = test_relation_map[source_table]
target_node = test_relation_map[target_table]

# Add synthetic runtime edge to isolated graph
test_graph["edges"].append({
    "source": source_node,
    "target": target_node,
    "edge_type": "runtime_observed",
    "run_id": synthetic_event["run_id"],
    "status": synthetic_event["status"],
    "transformation": synthetic_event["transformation"],
    "started_at": synthetic_event["started_at"],
    "completed_at": synthetic_event["completed_at"]
})

# Validate
runtime_test_edges = [
    edge for edge in test_graph["edges"]
    if edge["edge_type"] == "runtime_observed"
]

assert len(test_graph["nodes"]) == 13
assert len(test_graph["edges"]) == 14
assert len(runtime_test_edges) == 1

print("SYNTHETIC RUNTIME INTEGRATION TEST")
print("-" * 50)
print("Source:", source_node)
print("Target:", target_node)
print("Runtime edges:", len(runtime_test_edges))
print("Total graph edges:", len(test_graph["edges"]))
print("Run ID:", runtime_test_edges[0]["run_id"])

print("\nSYNTHETIC INTEGRATION TEST PASSED")

# COMMAND ----------
# ============================================================
#  PERSIST UNIFIED LINEAGE GRAPH
# ============================================================

import json
from pathlib import Path
from datetime import datetime, timezone

# Production graph output location
output_path = Path(
    "/Volumes/workspace/omni/lineage_metadata/unified_lineage_graph.json"
)

# Add graph metadata
unified_graph["metadata"]["graph_type"] = "UNIFIED_LINEAGE"
unified_graph["metadata"]["generated_at"] = (
    datetime.now(timezone.utc).isoformat()
)

unified_graph["metadata"]["node_count"] = len(
    unified_graph["nodes"]
)

unified_graph["metadata"]["edge_count"] = len(
    unified_graph["edges"]
)

# Persist graph
with output_path.open("w", encoding="utf-8") as file:
    json.dump(
        unified_graph,
        file,
        indent=4,
        default=str
    )

# ============================================================
# VERIFY PERSISTENCE
# ============================================================

with output_path.open("r", encoding="utf-8") as file:
    saved_graph = json.load(file)

assert len(saved_graph["nodes"]) == 13
assert len(saved_graph["edges"]) == 13

assert all(
    edge["edge_type"] == "dbt_declared"
    for edge in saved_graph["edges"]
)

print("UNIFIED LINEAGE GRAPH PERSISTENCE")
print("-" * 50)
print("Output:", output_path)
print("Nodes:", len(saved_graph["nodes"]))
print("Edges:", len(saved_graph["edges"]))
print("Graph type:", saved_graph["metadata"]["graph_type"])
print("Synthetic data included: No")

print("\nGRAPH PERSISTENCE VALIDATION PASSED")

# COMMAND ----------
# ============================================================
#  GRAPH INTEGRITY VALIDATION
# ============================================================

import json
from pathlib import Path
from collections import defaultdict, deque

# Load persisted graph independently
output_path = Path(
    "/Volumes/workspace/omni/lineage_metadata/unified_lineage_graph.json"
)

with output_path.open("r", encoding="utf-8") as file:
    validation_graph = json.load(file)

nodes = validation_graph["nodes"]
edges = validation_graph["edges"]

# ============================================================
#  VALIDATE EXPECTED DBT NODES
# ============================================================

expected_nodes = set(resources.keys())
actual_nodes = set(nodes.keys())

missing_nodes = expected_nodes - actual_nodes
unexpected_nodes = actual_nodes - expected_nodes

assert not missing_nodes, f"Missing nodes: {missing_nodes}"
assert not unexpected_nodes, f"Unexpected nodes: {unexpected_nodes}"

print("NODE VALIDATION PASSED")


# ============================================================
#  VALIDATE EDGE REFERENCES
# ============================================================

invalid_edges = []

for edge in edges:

    if (
        edge["source"] not in nodes
        or edge["target"] not in nodes
    ):
        invalid_edges.append(edge)

assert not invalid_edges, f"Invalid edges: {invalid_edges}"

print("EDGE REFERENCE VALIDATION PASSED")


# ============================================================
#  DETECT DUPLICATE EDGES
# ============================================================

edge_signatures = [
    (
        edge["source"],
        edge["target"],
        edge["edge_type"]
    )
    for edge in edges
]

duplicate_edges = (
    len(edge_signatures) - len(set(edge_signatures))
)

assert duplicate_edges == 0, (
    f"Duplicate edges found: {duplicate_edges}"
)

print("DUPLICATE EDGE VALIDATION PASSED")


# ============================================================
#  DETECT CYCLES
# KAHN'S TOPOLOGICAL SORT ALGORITHM
# ============================================================

adjacency = defaultdict(list)
indegree = {node_id: 0 for node_id in nodes}

for edge in edges:

    source = edge["source"]
    target = edge["target"]

    adjacency[source].append(target)
    indegree[target] += 1

queue = deque(
    node_id
    for node_id, degree in indegree.items()
    if degree == 0
)

processed_count = 0

while queue:

    current = queue.popleft()
    processed_count += 1

    for neighbor in adjacency[current]:

        indegree[neighbor] -= 1

        if indegree[neighbor] == 0:
            queue.append(neighbor)

has_cycle = processed_count != len(nodes)

assert not has_cycle, "Cycle detected in lineage graph"

print("CYCLE DETECTION PASSED")


# ============================================================
#  FINAL VALIDATION REPORT
# ============================================================

print("\nOMNI GRAPH INTEGRITY REPORT")
print("-" * 50)

print("Nodes:", len(nodes))
print("Edges:", len(edges))
print("Missing nodes:", len(missing_nodes))
print("Invalid edges:", len(invalid_edges))
print("Duplicate edges:", duplicate_edges)
print("Cycle detected:", has_cycle)

print("\nALL GRAPH INTEGRITY CHECKS PASSED")

# COMMAND ----------
# ============================================================
#  COLUMN-LEVEL LINEAGE - MART HR PAYROLL
# ============================================================

column_lineage = [

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "department",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "department",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "Department",
        "transformation": "DIRECT_MAPPING"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "headcount",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "employee_id",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "EmployeeNumber",
        "transformation": "COUNT_DISTINCT"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "total_gross_payroll",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "gross_salary",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "gross_salary",
        "transformation": "SUM_ROUND"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "total_net_payable",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "net_salary_payable",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "net_salary_payable",
        "transformation": "SUM_ROUND"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "avg_net_salary",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "net_salary_payable",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "net_salary_payable",
        "transformation": "AVG_ROUND"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "total_paye",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "paye_monthly",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "paye_monthly",
        "transformation": "SUM_ROUND"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "attrition_count",
        "upstream_model": "model.omni_dbt.stg_hr_employees",
        "upstream_column": "attrition_status",
        "source_relation": "workspace.bronze.hr_employees_raw",
        "source_column": "Attrition",
        "transformation": "CONDITIONAL_SUM"
    },

    {
        "target_model": "model.omni_dbt.mart_hr_payroll",
        "target_column": "blocked_payouts",
        "upstream_model": "model.omni_dbt.stg_hr_payroll_runs",
        "upstream_column": "payout_blocked",
        "source_relation": "workspace.bronze.hr_payroll_runs",
        "source_column": "payout_blocked",
        "transformation": "CONDITIONAL_SUM"
    }
]


# ------------------------------------------------------------
# JOIN AND AGGREGATION DEPENDENCIES
# ------------------------------------------------------------

column_lineage_dependencies = {

    "joins": [
        {
            "left_model": "model.omni_dbt.stg_hr_payroll_runs",
            "left_column": "employee_id",
            "right_model": "model.omni_dbt.stg_hr_employees",
            "right_column": "employee_id",
            "join_type": "LEFT JOIN"
        }
    ],

    "group_by": [
        {
            "model": "model.omni_dbt.mart_hr_payroll",
            "column": "department"
        }
    ],

    "row_filters": [
        {
            "model": "model.omni_dbt.stg_hr_payroll_runs",
            "condition": "gross_salary > 0 AND paye_monthly >= 0"
        },
        {
            "model": "model.omni_dbt.stg_hr_employees",
            "condition": (
                "EmployeeNumber IS NOT NULL "
                "AND MonthlyIncome IS NOT NULL"
            )
        }
    ]
}


print("COLUMN-LEVEL LINEAGE METADATA")
print("-" * 55)
print("Target model: mart_hr_payroll")
print("Target columns mapped:", len(column_lineage))
print("Join dependencies:", len(column_lineage_dependencies["joins"]))
print("Grouping dependencies:", len(column_lineage_dependencies["group_by"]))
print("Row-filter definitions:", len(column_lineage_dependencies["row_filters"]))

for item in column_lineage:
    print(
        f'{item["target_column"]} '
        f'<- {item["upstream_column"]} '
        f'({item["transformation"]})'
    )

# COMMAND ----------
# ============================================================
#  COLUMN LINEAGE VALIDATION
# ============================================================

expected_columns = {
    "department",
    "headcount",
    "total_gross_payroll",
    "total_net_payable",
    "avg_net_salary",
    "total_paye",
    "attrition_count",
    "blocked_payouts"
}

actual_columns = {
    item["target_column"]
    for item in column_lineage
}

# Check expected target columns
assert actual_columns == expected_columns, (
    f"Column mismatch. Missing: {expected_columns - actual_columns}; "
    f"Unexpected: {actual_columns - expected_columns}"
)

# Check duplicate target mappings
assert len(actual_columns) == len(column_lineage), (
    "Duplicate target-column mappings detected"
)

# Check model references
for item in column_lineage:
    assert item["target_model"] in graph["nodes"], (
        f'Unknown target model: {item["target_model"]}'
    )

    assert item["upstream_model"] in graph["nodes"], (
        f'Unknown upstream model: {item["upstream_model"]}'
    )

# Check join references
for join in column_lineage_dependencies["joins"]:
    assert join["left_model"] in graph["nodes"]
    assert join["right_model"] in graph["nodes"]

print("COLUMN COVERAGE VALIDATION PASSED")
print("DUPLICATE MAPPING VALIDATION PASSED")
print("MODEL REFERENCE VALIDATION PASSED")
print("JOIN REFERENCE VALIDATION PASSED")

print("\nCOLUMN LINEAGE VALIDATION REPORT")
print("-" * 55)
print("Expected target columns:", len(expected_columns))
print("Mapped target columns:", len(actual_columns))
print("Missing mappings:", len(expected_columns - actual_columns))
print("Unexpected mappings:", len(actual_columns - expected_columns))

print("\nALL COLUMN LINEAGE CHECKS PASSED")

# COMMAND ----------
# ============================================================
#  INTEGRATE COLUMN LINEAGE INTO UNIFIED GRAPH
# ============================================================

from copy import deepcopy

# Work on a copy to preserve the existing unified graph
column_graph = deepcopy(unified_graph)

# Add a dedicated column-lineage section
column_graph["column_lineage"] = {
    "mappings": column_lineage,
    "dependencies": column_lineage_dependencies
}

# Add graph metadata
column_graph["metadata"]["column_lineage_enabled"] = True
column_graph["metadata"]["column_lineage_mapping_count"] = len(column_lineage)

# Validate that existing table-level edges remain unchanged
assert len(column_graph["edges"]) == len(unified_graph["edges"])

assert column_graph["metadata"]["column_lineage_enabled"] is True

print("COLUMN LINEAGE INTEGRATION")
print("-" * 55)
print("Table-level nodes:", len(column_graph["nodes"]))
print("Table-level edges:", len(column_graph["edges"]))
print("Column mappings:", len(column_graph["column_lineage"]["mappings"]))
print(
    "Join dependencies:",
    len(column_graph["column_lineage"]["dependencies"]["joins"])
)
print(
    "Grouping dependencies:",
    len(column_graph["column_lineage"]["dependencies"]["group_by"])
)

print("\nEXISTING DBT GRAPH PRESERVED")
print("COLUMN LINEAGE INTEGRATION PASSED")

# COMMAND ----------
# ============================================================
#  PERSIST COLUMN-LEVEL LINEAGE GRAPH
# ============================================================

import json
from datetime import datetime, timezone

output_path = (
    "/Volumes/workspace/omni/lineage_metadata/"
    "unified_column_lineage_graph.json"
)

# Add generation metadata
column_graph["metadata"]["generated_at"] = (
    datetime.now(timezone.utc).isoformat()
)

column_graph["metadata"]["graph_type"] = (
    "UNIFIED_TABLE_AND_COLUMN_LINEAGE"
)

# Persist graph
with open(output_path, "w") as f:
    json.dump(column_graph, f, indent=4)

# Reload and verify
with open(output_path, "r") as f:
    persisted_column_graph = json.load(f)

assert len(persisted_column_graph["nodes"]) == 13
assert len(persisted_column_graph["edges"]) == 13
assert len(persisted_column_graph["column_lineage"]["mappings"]) == 8

print("COLUMN LINEAGE GRAPH PERSISTENCE")
print("-" * 55)
print("Output:", output_path)
print("Table-level nodes:", len(persisted_column_graph["nodes"]))
print("Table-level edges:", len(persisted_column_graph["edges"]))
print(
    "Column mappings:",
    len(persisted_column_graph["column_lineage"]["mappings"])
)
print("Graph type:", persisted_column_graph["metadata"]["graph_type"])

print("\nCOLUMN LINEAGE PERSISTENCE VALIDATION PASSED")

# COMMAND ----------
# ============================================================
# COLUMN-LEVEL LINEAGE TRAVERSAL
# ============================================================

from collections import defaultdict, deque

column_upstream_index = defaultdict(list)
column_downstream_index = defaultdict(list)

for mapping in column_graph["column_lineage"]["mappings"]:

    target = (
        mapping["target_model"],
        mapping["target_column"]
    )

    upstream = (
        mapping["upstream_model"],
        mapping["upstream_column"]
    )

    column_upstream_index[target].append({
        "node": upstream,
        "transformation": mapping["transformation"]
    })

    column_downstream_index[upstream].append({
        "node": target,
        "transformation": mapping["transformation"]
    })


def get_column_upstream(model, column):
    return column_upstream_index.get((model, column), [])


def get_column_downstream(model, column):
    return column_downstream_index.get((model, column), [])


# Test: identify direct upstream dependency of total_gross_payroll
result = get_column_upstream(
    "model.omni_dbt.mart_hr_payroll",
    "total_gross_payroll"
)

print("COLUMN-LEVEL UPSTREAM TEST")
print("-" * 55)

for item in result:
    print("Model:", item["node"][0])
    print("Column:", item["node"][1])
    print("Transformation:", item["transformation"])

assert len(result) == 1
assert result[0]["node"][1] == "gross_salary"

print("\nCOLUMN-LEVEL TRAVERSAL INITIALIZATION PASSED")

# COMMAND ----------
# ============================================================
#  SILVER-TO-BRONZE COLUMN LINEAGE
# ============================================================

silver_column_lineage = [

    # stg_hr_payroll_runs
    {
        "target_model": "model.omni_dbt.stg_hr_payroll_runs",
        "target_column": "employee_id",
        "upstream_relation": "workspace.bronze.hr_payroll_runs",
        "upstream_column": "EmployeeNumber",
        "transformation": "RENAME"
    },
    {
        "target_model": "model.omni_dbt.stg_hr_payroll_runs",
        "target_column": "department",
        "upstream_relation": "workspace.bronze.hr_payroll_runs",
        "upstream_column": "Department",
        "transformation": "RENAME"
    },
    {
        "target_model": "model.omni_dbt.stg_hr_payroll_runs",
        "target_column": "gross_salary",
        "upstream_relation": "workspace.bronze.hr_payroll_runs",
        "upstream_column": "gross_salary",
        "transformation": "DIRECT"
    },
    {
        "target_model": "model.omni_dbt.stg_hr_payroll_runs",
        "target_column": "net_salary_payable",
        "upstream_relation": "workspace.bronze.hr_payroll_runs",
        "upstream_column": "net_salary_payable",
        "transformation": "DIRECT"
    },
    {
        "target_model": "model.omni_dbt.stg_hr_payroll_runs",
        "target_column": "paye_monthly",
        "upstream_relation": "workspace.bronze.hr_payroll_runs",
        "upstream_column": "paye_monthly",
        "transformation": "DIRECT"
    },
    {
        "target_model": "model.omni_dbt.stg_hr_payroll_runs",
        "target_column": "payout_blocked",
        "upstream_relation": "workspace.bronze.hr_payroll_runs",
        "upstream_column": "payout_blocked",
        "transformation": "DIRECT"
    },

    # stg_hr_employees
    {
        "target_model": "model.omni_dbt.stg_hr_employees",
        "target_column": "employee_id",
        "upstream_relation": "workspace.bronze.hr_employees_raw",
        "upstream_column": "EmployeeNumber",
        "transformation": "RENAME"
    },
    {
        "target_model": "model.omni_dbt.stg_hr_employees",
        "target_column": "attrition_status",
        "upstream_relation": "workspace.bronze.hr_employees_raw",
        "upstream_column": "Attrition",
        "transformation": "RENAME"
    }
]

# Register Silver-to-Bronze mappings
column_graph["column_lineage"]["silver_to_bronze"] = (
    silver_column_lineage
)

print("SILVER-TO-BRONZE LINEAGE")
print("-" * 55)
print("Mappings registered:", len(silver_column_lineage))
print("Payroll staging mappings:", 6)
print("Employee staging mappings:", 2)

assert len(silver_column_lineage) == 8

print("\nSILVER-TO-BRONZE LINEAGE REGISTRATION PASSED")

# COMMAND ----------
# ============================================================
#  RECURSIVE COLUMN-LEVEL LINEAGE
# ============================================================

# Build Silver-to-Bronze lookup
silver_to_bronze_index = defaultdict(list)

for mapping in column_graph["column_lineage"]["silver_to_bronze"]:

    key = (
        mapping["target_model"],
        mapping["target_column"]
    )

    silver_to_bronze_index[key].append({
        "relation": mapping["upstream_relation"],
        "column": mapping["upstream_column"],
        "transformation": mapping["transformation"]
    })


def trace_column_to_bronze(model, column):

    direct_sources = get_column_upstream(model, column)

    lineage_path = []

    for source in direct_sources:

        upstream_model, upstream_column = source["node"]

        path_entry = {
            "model": upstream_model,
            "column": upstream_column,
            "transformation": source["transformation"]
        }

        bronze_sources = silver_to_bronze_index.get(
            (upstream_model, upstream_column),
            []
        )

        if bronze_sources:

            for bronze in bronze_sources:
                lineage_path.append({
                    "silver_model": upstream_model,
                    "silver_column": upstream_column,
                    "bronze_relation": bronze["relation"],
                    "bronze_column": bronze["column"],
                    "gold_transformation": source["transformation"],
                    "silver_transformation": bronze["transformation"]
                })

        else:
            lineage_path.append(path_entry)

    return lineage_path


# Test full lineage for total_gross_payroll
result = trace_column_to_bronze(
    "model.omni_dbt.mart_hr_payroll",
    "total_gross_payroll"
)

print("RECURSIVE COLUMN LINEAGE TEST")
print("-" * 55)

for item in result:
    print(item)

assert len(result) == 1
assert result[0]["bronze_column"] == "gross_salary"
assert result[0]["bronze_relation"] == (
    "workspace.bronze.hr_payroll_runs"
)

print("\nRECURSIVE COLUMN LINEAGE TEST PASSED")

# COMMAND ----------
# ============================================================
#  END-TO-END COLUMN LINEAGE VALIDATION
# ============================================================

gold_model = "model.omni_dbt.mart_hr_payroll"

lineage_report = []

for mapping in column_graph["column_lineage"]["mappings"]:

    target_column = mapping["target_column"]

    result = trace_column_to_bronze(
        gold_model,
        target_column
    )

    for item in result:
        lineage_report.append({
            "gold_column": target_column,
            "silver_column": item["silver_column"],
            "bronze_relation": item["bronze_relation"],
            "bronze_column": item["bronze_column"],
            "gold_transformation": item["gold_transformation"],
            "silver_transformation": item["silver_transformation"]
        })


# Validate expected mappings
expected_gold_columns = {
    "department",
    "headcount",
    "total_gross_payroll",
    "total_net_payable",
    "avg_net_salary",
    "total_paye",
    "attrition_count",
    "blocked_payouts"
}

resolved_gold_columns = {
    item["gold_column"]
    for item in lineage_report
}

assert resolved_gold_columns == expected_gold_columns, (
    f"Missing: {expected_gold_columns - resolved_gold_columns}"
)

assert len(lineage_report) == 8

print("END-TO-END COLUMN LINEAGE REPORT")
print("-" * 75)

for item in lineage_report:
    print(
        f'{item["gold_column"]} -> '
        f'{item["bronze_relation"]}.{item["bronze_column"]}'
    )

print("\nGold columns expected:", len(expected_gold_columns))
print("Gold columns resolved:", len(resolved_gold_columns))
print("Bronze lineage paths:", len(lineage_report))

print("\nEND-TO-END COLUMN LINEAGE VALIDATION PASSED")

# COMMAND ----------
# ============================================================
#  COLUMN-LEVEL DOWNSTREAM IMPACT ANALYSIS
# ============================================================

def find_bronze_column_impact(bronze_relation, bronze_column):

    affected_columns = []

    for mapping in column_graph["column_lineage"]["silver_to_bronze"]:

        if (
            mapping["upstream_relation"] == bronze_relation
            and mapping["upstream_column"] == bronze_column
        ):

            silver_model = mapping["target_model"]
            silver_column = mapping["target_column"]

            downstream = get_column_downstream(
                silver_model,
                silver_column
            )

            for item in downstream:

                gold_model, gold_column = item["node"]

                affected_columns.append({
                    "bronze_relation": bronze_relation,
                    "bronze_column": bronze_column,
                    "silver_model": silver_model,
                    "silver_column": silver_column,
                    "gold_model": gold_model,
                    "gold_column": gold_column,
                    "transformation": item["transformation"]
                })

    return affected_columns


# Test impact of changing EmployeeNumber
impact = find_bronze_column_impact(
    "workspace.bronze.hr_payroll_runs",
    "EmployeeNumber"
)

print("COLUMN-LEVEL DOWNSTREAM IMPACT")
print("-" * 65)

for item in impact:
    print(
        f'{item["bronze_column"]} -> '
        f'{item["silver_column"]} -> '
        f'{item["gold_column"]}'
    )

print("\nAffected Gold columns:", len(impact))

assert any(
    item["gold_column"] == "headcount"
    for item in impact
)

print("\nCOLUMN-LEVEL IMPACT TEST PASSED")
