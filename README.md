# UK Procurement Data Pipeline

An end-to-end data engineering project that ingests UK public procurement data from the **Find a Tender Service**, transforms it into an analytics-ready data model, and makes the resulting data available for reporting in **Power BI**.

The project demonstrates a production-style daily batch pipeline using incremental ingestion, cloud-hosted analytical storage, dbt transformations, data quality testing, orchestration and BI reporting.

## Project Overview

UK public procurement information is published through the Find a Tender Service using the Open Contracting Data Standard (OCDS).

The source data is useful but presents several challenges for analysis:

- A procurement process can have multiple releases over time.
- Releases contain nested JSON structures.
- Buyers and suppliers can have many-to-many relationships with procurements.
- Records can be updated after their initial publication.
- Important fields such as procurement value are not always populated.
- Raw source fields require transformation before they are suitable for BI reporting.

This project builds an automated pipeline that preserves the raw release history while producing clean, current-state analytical models.

## Architecture

```text
Find a Tender API
        │
        ▼
Python Ingestion
        │
        │ Incremental extraction
        │ Watermarking
        │ Pagination
        │ Retry / rate-limit handling
        ▼
MotherDuck / DuckDB
        │
        │ Raw OCDS releases
        │ Pipeline audit history
        ▼
dbt
        │
        ├── Staging models
        ├── Current-state models
        ├── Buyer & supplier dimensions
        ├── Relationship bridges
        ├── Data quality tests
        └── Gold reporting models
        │
        ▼
Power BI
        │
        ▼
UK Procurement Intelligence Dashboard

             ▲
             │
       Prefect Cloud
   Daily orchestration
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | API ingestion and pipeline logic |
| Find a Tender API | Source procurement data |
| DuckDB / MotherDuck | Analytical database and cloud storage |
| dbt | SQL transformation, modelling and testing |
| Prefect Cloud | Pipeline orchestration and scheduling |
| GitHub | Source control and deployment source |
| Power BI | Analytics and dashboarding |

## Data Ingestion

The ingestion layer retrieves OCDS release packages from the Find a Tender API.

Rather than downloading the entire dataset every day, the pipeline uses an **incremental watermark strategy**.

The most recent successfully processed release timestamp is stored and supplied to the API as `updatedFrom` on the next run.

The ingestion process includes:

- API pagination
- Incremental extraction
- Watermark management
- Duplicate protection
- API rate-limit handling
- Retry logic
- Pipeline run auditing
- Failure handling

Raw releases are stored without overwriting previous releases, preserving the history of changes to procurement processes.

The `release_id` acts as the unique key and duplicate releases are ignored during loading.

## Pipeline Auditing

Each ingestion run is recorded in a `pipeline_runs` table.

The audit information includes:

- Run ID
- Start and completion time
- Starting watermark
- Ending watermark
- Rows extracted
- Rows inserted
- Rows skipped
- Pipeline status

Only successful runs advance the ingestion watermark, allowing failed runs to be safely retried.

## Data Modelling with dbt

The raw OCDS JSON is transformed through a series of dbt models.

The project separates transformation into staging, current-state, dimensional and reporting layers.

### Staging

Staging models extract useful entities and attributes from the raw JSON, including:

- Procurement releases
- Awards
- Award suppliers
- Buyers

### Current State

A single procurement process (`ocid`) can have multiple releases.

Current-state models identify the latest available representation of each procurement or award while the raw layer retains the complete release history.

### Buyer Model

Procurements can involve more than one buyer, so buyer information is modelled separately rather than flattening it into the procurement fact table.

```text
dim_buyers
     │
     │ buyer_id
     ▼
procurement_buyers
     │
     │ ocid
     ▼
gold_procurement_overview
```

This allows buyer attributes such as location to filter procurement reporting without duplicating procurement values.

### Supplier Model

Supplier relationships are similarly extracted from award information so suppliers can be analysed independently of the raw OCDS structure.

## Gold Reporting Layer

The final dbt models provide simplified datasets designed for reporting.

### `gold_procurement_overview`

Contains one row per current procurement and includes attributes such as:

- OCID
- Latest release
- Release date
- Buyer
- Tender title
- Tender status
- Procurement method
- Tender value
- Currency

Keeping this model at **one row per procurement** prevents financial values from being duplicated when a procurement has multiple buyers.

### `gold_daily_procurement_activity`

Provides daily pipeline/reporting metrics including:

- New procurements
- Updated procurements

A procurement is considered new on the date of its first release. Later releases on subsequent dates are treated as updates.

## Data Quality

dbt tests are used to validate assumptions about the transformed data.

Current tests include checks for:

- Required identifiers
- Unique dimension keys
- Valid procurement relationships
- Buyer identifiers
- Bridge-table integrity

The project currently runs **28 dbt data quality tests** as part of the automated pipeline.

If a dbt build or test fails, Prefect records the pipeline failure rather than silently publishing an apparently successful run.

## Orchestration

The pipeline is orchestrated using **Prefect Cloud**.

The production flow runs:

```text
1. Incremental API ingestion
           ↓
2. dbt model build
           ↓
3. dbt data quality tests
```

The deployment runs automatically each morning using Prefect's serverless infrastructure.

Pipeline code is retrieved from the GitHub repository at runtime, allowing changes committed to the main branch to be picked up by future pipeline runs.

Secrets such as the MotherDuck access token are stored securely in Prefect rather than committed to source control.

## Power BI Dashboard

The Gold layer is consumed by Power BI using MotherDuck's PostgreSQL-compatible interface.

The dashboard currently includes:

- Total procurements
- Unique buyers
- Percentage of procurements containing tender values
- New vs updated procurements over time
- Procurement status distribution
- Procurement method distribution
- Buyer geography filtering

The reporting layer has also highlighted source-data quality issues. For example, only a subset of procurement records contains a published tender value, so value-based KPIs need to be interpreted carefully rather than assuming complete financial coverage.

## Key Engineering Concepts Demonstrated

This project was designed as a practical data engineering portfolio project rather than simply a dashboard.

It demonstrates:

- REST API ingestion
- Nested JSON processing
- Pagination
- Incremental data loading
- Watermarking
- Idempotent loading
- Historical data preservation
- Retry and rate-limit handling
- Pipeline auditing
- ELT architecture
- Dimensional modelling
- Many-to-many bridge modelling
- dbt transformations
- Automated data quality testing
- Cloud analytical storage
- Workflow orchestration
- Secret management
- Scheduled cloud execution
- BI semantic modelling
- Data quality analysis

## Repository Structure

```text
procurement-data-pipeline/
│
├── ingestion.py
├── pipeline.py
├── prefect.yaml
├── requirements.txt
├── README.md
│
├── procurement_analytics/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── gold/
│   └── ...
│
├── notebooks/
│   └── 04_gold_layer_exploration.ipynb
│
└── powerbi/
    └── UK Procurement Intelligence Dashboard.pbix
```

> The exact repository structure may evolve as the project is refined.

## Running Locally

Create and activate a Python virtual environment, then install the dependencies:

```bash
pip install -r requirements.txt
```

The pipeline requires access to a MotherDuck database.

Set the MotherDuck token as an environment variable rather than storing credentials in source code:

```text
MOTHERDUCK_TOKEN=<your token>
```

Run