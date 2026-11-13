# OMNI — Financial Data Engineering & Lineage Platform

OMNI is a Databricks-based data engineering project for HR, payroll, and finance data. It implements a Bronze–Silver–Gold medallion architecture, dbt transformations, payroll and statutory-deduction processing, finance-oriented data marts, and a developing metadata and data-lineage capability.

The project is designed to make financial data transformations easier to inspect, validate, trace, and audit. Its lineage work combines dbt-declared table dependencies with a lightweight runtime event tracker and a manually curated column-lineage layer.

> **Project status:** The table-level dbt graph and the first column-level lineage slice for `mart_hr_payroll` have been implemented and validated. Runtime event ingestion and integration mechanisms are present, but a production payroll run has not yet generated a production runtime event file. Column-level lineage is not yet complete for all dbt models.

---

## Contents

- [Project objectives](#project-objectives)
- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Data layers and models](#data-layers-and-models)
- [Lineage and metadata capabilities](#lineage-and-metadata-capabilities)
- [Current validation results](#current-validation-results)
- [Repository structure](#repository-structure)
- [Environment and setup](#environment-and-setup)
- [Running dbt](#running-dbt)
- [Lineage metadata artifacts](#lineage-metadata-artifacts)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)
- [Engineering principles](#engineering-principles)

---

## Project objectives

OMNI brings together several data-engineering concerns in a financial-data context:

1. **Ingestion:** land HR, payroll, and foreign-exchange data in a Bronze layer.
2. **Transformation:** clean and validate source records in Silver.
3. **Business modelling:** produce Gold models for payroll reporting, PAYE analysis, remittance planning, journal entries, and market rates.
4. **Data quality:** preserve invalid or rejected records in a quarantine model and apply validation checks.
5. **Metadata and lineage:** represent dbt model dependencies, record runtime pipeline events, and trace selected Gold columns to their upstream columns.
6. **Impact analysis:** identify downstream table and column dependencies when an upstream asset changes.

The lineage capability is being developed as an engineering feature of the data platform rather than as a claim that every transformation is already captured automatically.

---

## Architecture

OMNI follows a medallion architecture:

```text
HR / Payroll / Market Data
          |
          v
+---------------------------+
|          BRONZE           |
| Raw ingested Delta tables |
+---------------------------+
          |
          v
+---------------------------+
|          SILVER           |
| dbt staging and validation|
+---------------------------+
          |
          +--------------------------+
          |                          |
          v                          v
+-------------------+      +----------------------+
|    QUARANTINE     |      |         GOLD         |
| Rejected records  |      | Finance and HR marts |
+-------------------+      +----------------------+
                                      |
                                      v
                           Reporting / downstream use

Metadata and lineage:
  dbt manifest ---------> declared table-level graph
  Runtime tracker ------> JSONL execution events
  Curated column maps --> selected column-level lineage
                                      |
                                      v
                           Lineage graph artifacts
```

### Bronze

The Bronze layer contains ingested source data. The dbt manifest currently declares these three sources:

| Source | Purpose |
|---|---|
| `bronze.hr_employees_raw` | Raw employee and HR attributes |
| `bronze.hr_payroll_runs` | Payroll calculation output and payout information |
| `bronze.market_exchange_rates_raw` | Ingested foreign-exchange rates |

### Silver

Silver models expose cleaned and validated records for downstream transformations:

| Model | Purpose |
|---|---|
| `stg_hr_employees` | Employee attributes, including department, job grade, income, attrition, and tenure |
| `stg_hr_payroll_runs` | Payroll records with employee identifiers, salary components, deductions, payout status, and run metadata |
| `stg_market_exchange_rates` | Standardized market exchange-rate records |

The HR staging models apply source-level filters. For example, the payroll staging model retains records where `gross_salary > 0` and `paye_monthly >= 0`. The employee staging model requires non-null employee numbers and monthly income.

### Quarantine

| Model | Purpose |
|---|---|
| `quarantine_all_bad_records` | Consolidates rejected or invalid records for inspection and remediation |

### Gold

The current dbt manifest contains six Gold models:

| Model | Purpose |
|---|---|
| `mart_hr_paye_detail` | Employee-level PAYE and payroll detail |
| `mart_hr_paye_summary` | Aggregated PAYE reporting |
| `mart_hr_payroll` | Department-level headcount, payroll cost, PAYE, attrition, and blocked-payout metrics |
| `mart_finance_journal_entries` | Finance journal-entry modelling from payroll data |
| `mart_finance_remittance_schedule` | Statutory remittance schedule modelling |
| `mart_market_rates` | Market-rate reporting |

---

## Technology stack

| Area | Technology |
|---|---|
| Cloud data platform | Databricks |
| Distributed processing | Apache Spark / PySpark |
| Storage and table format | Delta Lake |
| Governance and storage namespace | Unity Catalog |
| Transformation framework | dbt Core |
| Databricks adapter | dbt-databricks |
| Metadata source | dbt `manifest.json` |
| Runtime event format | JSON Lines (`.jsonl`) |
| Lineage implementation | Python |
| Version control | Git and GitHub |

The validated dbt environment used dbt Core **1.11.6** and dbt-databricks **1.11.5**.

---

## Lineage and metadata capabilities

The lineage implementation is organized into distinct layers so that declared dependencies, execution evidence, and column mappings are not conflated.

### 1. dbt-declared table-level lineage

The lineage notebook reads the dbt manifest and constructs a directed graph from dbt model dependencies.

Current manifest-derived graph:

- **13 nodes:** 10 dbt models and 3 dbt sources.
- **13 directed dependency edges.**
- Separate node identities for models and sources.
- Upstream traversal for a selected model.
- Downstream impact traversal for a selected source or model.

The graph preserves dbt dependency direction: an edge points from an upstream dependency to the model that depends on it.

### 2. Runtime event tracking

The Python `LineageTracker` records execution metadata such as:

- Pipeline and transformation names.
- Script or notebook identifier.
- Run ID.
- Start and end timestamps.
- Duration and execution status.
- Declared input and output tables.
- Transformation metadata.
- Error details for failed executions.

Events are written as JSON Lines, allowing each execution to be appended and read independently.

The tracker supports successful and failed execution capture through its context-manager interface. The current implementation records table-level input/output relationships; it does not independently prove column-level causality.

**Runtime evidence status:** the tracker has passed an isolated smoke test, and the graph notebook has a runtime-event ingestion and integration path. At the last validation, the production `lineage_events.jsonl` file did not exist because the production payroll job had not yet been run with the updated tracker. Synthetic integration testing is kept separate from production evidence.

### 3. Column-level lineage

The first manually curated column-lineage implementation targets:

`model.omni_dbt.mart_hr_payroll`

It records eight Gold output-column mappings:

| Gold column | Silver dependency | Transformation |
|---|---|---|
| `department` | `stg_hr_payroll_runs.department` | Direct mapping |
| `headcount` | `stg_hr_payroll_runs.employee_id` | `COUNT(DISTINCT)` |
| `total_gross_payroll` | `stg_hr_payroll_runs.gross_salary` | `SUM`, `ROUND` |
| `total_net_payable` | `stg_hr_payroll_runs.net_salary_payable` | `SUM`, `ROUND` |
| `avg_net_salary` | `stg_hr_payroll_runs.net_salary_payable` | `AVG`, `ROUND` |
| `total_paye` | `stg_hr_payroll_runs.paye_monthly` | `SUM`, `ROUND` |
| `attrition_count` | `stg_hr_employees.attrition_status` | Conditional `SUM` |
| `blocked_payouts` | `stg_hr_payroll_runs.payout_blocked` | Conditional `SUM` |

The implementation also records:

- The `LEFT JOIN` between payroll and employee staging models on `employee_id`.
- The `GROUP BY department` aggregation context.
- The row filters applied in the two staging models.
- Targeted Silver-to-Bronze mappings for the columns needed by this Gold model.
- Direct upstream traversal and an initial downstream column-impact query.

The mappings are manually curated from inspected SQL, not generated by a general SQL parser. The current impact-analysis function follows registered direct mappings; join, grouping, and filter dependencies are recorded separately and are not yet fully incorporated into a general dependency-aware impact engine.

### 4. Graph integrity

The table-level graph validation checks:

- Expected nodes are present.
- Every edge references existing nodes.
- Duplicate edges are absent.
- Cycles are detected using a topological-sort check.

The persisted dbt graph passed these checks with 13 nodes, 13 edges, no invalid references, no duplicate edges, and no detected cycle.

---

## Current validation results

The following results were observed during development:

| Validation | Result |
|---|---|
| dbt manifest ingestion | Passed |
| Table graph construction | 13 nodes / 13 edges |
| Upstream and downstream table traversal | Passed |
| Graph persistence and reload | Passed |
| Node and edge-reference integrity | Passed |
| Duplicate-edge check | Passed |
| Cycle detection | Passed; no cycle detected |
| Runtime tracker isolated smoke test | Passed |
| Synthetic runtime graph integration | Passed; kept separate from production data |
| Column mapping coverage for `mart_hr_payroll` | 8 of 8 target columns |
| Column mapping structural validation | Passed |
| Silver-to-Bronze targeted mapping registration | 8 mappings |
| Gold-to-Bronze trace test | Passed for the eight registered paths |
| Initial column-level impact test | Passed for the tested `EmployeeNumber` dependency |

### Payroll join-grain checks

The current staging data was also checked for duplicate employee keys:

| Check | Observed result |
|---|---:|
| Employee staging rows | 1,470 |
| Distinct employee IDs | 1,470 |
| Employee staging duplicate rows | 0 |
| Payroll employee IDs with multiple records | No rows returned |
| Duplicate employee-key query | No rows returned |

These results describe the data snapshot checked during development; they are not a permanent guarantee. dbt uniqueness tests should be used to enforce the intended key constraints on future runs.

---

## Repository structure

The repository includes the Databricks notebooks, the dbt project, and supporting lineage code. The principal lineage-related assets are:

```text
omni/
├── notebooks/
│   ├── 03_paye_payroll.py
│   ├── 04_unified_lineage_graph.py
│   └── lineage_tracker.py
│
├── Omni_dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/
│   │   │   ├── hr/
│   │   │   └── market/
│   │   ├── quarantine/
│   │   └── gold/
│   └── tests/
│
└── README.md
```

This is a logical overview of the principal project assets; ancillary files and deployment configuration may also exist in the repository.

---

## Environment and setup

### Prerequisites

- Access to a Databricks workspace.
- A Unity Catalog-enabled environment with permission to read source tables and write to the project metadata volume.
- Python and the project dependencies used by the notebooks.
- dbt Core and the Databricks adapter compatible with the project configuration.
- Git for version control.

### Metadata storage

The lineage notebooks use this Unity Catalog Volume path:

```text
/Volumes/workspace/omni/lineage_metadata/
```

The directory is used for lineage metadata artifacts, including the dbt manifest, runtime event log, and graph JSON files.

The volume and its permissions must be configured in the target workspace before running the lineage notebooks. Do not commit credentials, API keys, access tokens, or private data to Git.

### Configure the dbt connection

Review `Omni_dbt/profiles.yml` or the applicable dbt profile configuration in your environment. Configure Databricks host, HTTP path, authentication, catalog, and schema using secure environment-specific settings. Do not store secrets in the repository.

---

## Running dbt

From the dbt project directory, install dependencies and validate the project:

```bash
cd Omni_dbt
dbt deps
dbt debug
dbt parse
```

To build the models and execute configured tests:

```bash
dbt build
```

Alternatively, run transformations and tests separately:

```bash
dbt run
dbt test
```

Use the target and profile appropriate to your Databricks environment. Review the generated `target/manifest.json` after parsing or building; the lineage notebook consumes the manifest generated by the dbt project.

---

## Lineage metadata artifacts

The lineage workflow uses the following artifacts in the Unity Catalog Volume:

| Artifact | Role |
|---|---|
| `manifest.json` | dbt project metadata and declared dependencies |
| `lineage_events.jsonl` | Runtime execution events written by the tracker when instrumented jobs run |
| `unified_lineage_graph.json` | Persisted table-level graph built from dbt-declared dependencies |
| `unified_column_lineage_graph.json` | Column-lineage graph artifact for the current curated implementation |

The graph notebook is designed to load the manifest, build and validate the table graph, ingest runtime events when present, add curated column mappings, and persist graph artifacts.

After changing the in-memory column-lineage metadata, rerun the persistence cell so the JSON artifact reflects the latest mappings. A successful notebook cell does not automatically update a previously written artifact.

---

## Known limitations

The following are explicit boundaries of the current implementation:

1. **Column coverage is partial.** The first column-lineage slice covers `mart_hr_payroll`, not all ten dbt models.
2. **Mappings are curated.** Column dependencies are manually registered from inspected SQL; a general SQL parser or automatic dbt column-lineage extractor has not yet been implemented.
3. **Impact analysis is initial.** Direct mappings are traversed. Join, grouping, filter, and indirect dependencies still need to be incorporated into a comprehensive impact engine.
4. **Runtime production evidence is pending.** The event tracker and ingestion mechanism exist, but production events have not yet been confirmed from a payroll job execution using the updated tracker.
5. **Runtime edges are table-level.** Input/output event relationships do not establish exact column-level causality.
6. **Production job migration is pending validation.** The existing payroll job should remain unchanged until the updated notebook and tracker have been validated, with a rollback path prepared.
7. **Data-quality checks reflect a snapshot.** The observed uniqueness results should be converted into repeatable dbt tests.

---

## Roadmap

- [x] Ingest dbt manifest metadata.
- [x] Build the dbt-declared table-level dependency graph.
- [x] Implement upstream lineage and downstream table impact traversal.
- [x] Add runtime event tracking and JSONL ingestion mechanisms.
- [x] Persist and validate the table-level graph.
- [x] Register the first eight Gold column mappings for `mart_hr_payroll`.
- [x] Add targeted Silver-to-Bronze column mappings for the payroll mart.
- [x] Validate the initial Gold-to-Bronze lineage paths.
- [x] Implement an initial column-level downstream impact query.
- [ ] Incorporate join, grouping, and filter dependencies into impact analysis.
- [ ] Expand column-level lineage to the remaining dbt models.
- [ ] Add automated tests for column mappings and lineage completeness.
- [ ] Validate real success and failure runtime events from an approved execution.
- [ ] Integrate table-level, column-level, and runtime lineage into a consistently refreshed graph artifact.
- [ ] Validate production job migration and rollback procedure.
- [ ] Complete architecture, operational, and deployment documentation.

---

## Engineering principles

OMNI's lineage implementation follows several design principles:

- **Separate declared from observed lineage.** dbt dependencies and runtime events represent different kinds of evidence and remain distinguishable.
- **Preserve source-to-target traceability.** Mappings identify the model, column, and transformation involved.
- **Do not overstate lineage coverage.** A tested mapping for one model is not equivalent to platform-wide column lineage.
- **Keep synthetic tests separate.** Synthetic fixtures validate mechanics; they are not production execution evidence.
- **Protect production workflows.** Existing payroll jobs should not be changed until the replacement path has been validated and a rollback is available.
- **Make metadata inspectable.** JSON and JSONL artifacts allow lineage records to be examined and validated independently.

---

## Contributing

When extending OMNI:

1. Keep dbt model names and source definitions consistent with the manifest.
2. Add or update tests when changing model grain, keys, or transformation logic.
3. Update column mappings when SQL transformations change.
4. Preserve the distinction between declared dependencies, runtime-observed relationships, and curated column mappings.
5. Validate graph integrity and refresh persisted metadata artifacts after changes.
6. Never commit secrets, credentials, or sensitive source data.

---

## License

Add the applicable license before redistributing or reusing this project.
