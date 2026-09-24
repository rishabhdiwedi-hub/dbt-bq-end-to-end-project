# dbt BigQuery End-to-End Walmart Data Project

An end-to-end analytics engineering project that demonstrates how Walmart order data can be moved from raw BigQuery source tables through incremental dbt transformations, a business-ready silver one-big-table (OBT) model, and gold fact/dimension-style outputs. Apache Airflow orchestrates the dbt workflow in Docker, while dbt-bigquery executes the transformations in Google BigQuery.

> **Project status:** This repository is a learning/development implementation. The Airflow compose file is explicitly intended for local development, and the orchestration DAG contains a few commands and paths that should be corrected before relying on it for production scheduling. See [Known implementation notes](#known-implementation-notes).

## What this project demonstrates

- Defining BigQuery sources with dbt's `source()` function.
- Incremental loading using `updated_timestamp` watermarks and business-key `unique_key` values.
- Layered warehouse modeling:
  - **Raw/source:** Walmart operational tables in BigQuery.
  - **Silver technical:** one incremental model per source entity.
  - **Silver business:** a joined OBT containing order, customer, product, store, employee, and order-item information.
  - **Gold:** reusable ephemeral customer/employee/order/product/store models and an `fact_orders` output.
- Metadata-driven SQL generation in `models/silver_obt_b/silver_obt.sql`.
- Schema placement and materialization control through `dbt_project.yml` and the `generate_schema_name` macro.
- Data quality checks for identifiers and a custom null-key test.
- Airflow task dependencies for freshness checks, transformations, tests, and gold-layer publication.

## Architecture and end-to-end flow

```text
Walmart operational data in BigQuery
        |
        |  dbt source definitions: models/source/sources.yml
        v
Raw source tables: customers, orders, employees, products,
                   order_items, stores
        |
        |  incremental dbt models; updated_timestamp watermark
        v
Silver technical: walmart_silver.*_t
        |
        |  metadata-driven LEFT JOINs in silver_obt.sql
        v
Silver business OBT: walmart_silver.silver_obt
        |
        +--> Gold ephemeral models: customer, employee, order, product, store views
        |
        +--> Gold fact_orders: order-level analytical fact fields

Airflow (Docker/Celery) runs the workflow:
CDC placeholder -> cleanup -> source freshness -> silver run/test
-> silver OBT run/test -> gold models -> snapshot/fact publication
```

### 1. Source layer

`walmart_project/models/source/sources.yml` registers two BigQuery source groups under the configured project:

- `walmart.wallmart`: `customers`, `orders`, `employees`, `products`, `order_items`, and `stores`.
- `walmart_silver.walmart_silver`: the silver outputs, including `silver_obt` and each technical silver table.

The source definitions allow models to reference upstream data with expressions such as `{{ source('walmart', 'orders') }}` and allow dbt to perform source freshness checks.

### 2. Silver technical layer

The models in `walmart_project/models/silver_t/` are incremental models with a business key and a processing timestamp. The entity keys are:

| Model | Unique key |
| --- | --- |
| `customers_t` | `customer_id` |
| `orders_t` | `order_id` |
| `employees_t` | `employee_id` |
| `products_t` | `product_id` |
| `order_items_t` | `order_item_id` |
| `stores_t` | `store_id` |

Each model selects the source row, adds `current_timestamp() AS processed_at`, and on subsequent runs filters rows with:

```sql
updated_timestamp > (
  SELECT COALESCE(MAX(updated_timestamp), '1900-01-01')
  FROM {{ this }}
)
```

This reduces the amount of source data processed after the initial load. The current SQL assumes that every upstream table contains a reliable `updated_timestamp` column and that the configured `unique_key` identifies a row for the selected incremental strategy.

`models/silver_t/properties.yml` adds checks including non-null and uniqueness tests for product and store identifiers, plus a non-null test for `product_name`.

### 3. Silver business OBT

`models/silver_obt_b/silver_obt.sql` uses a Jinja `configs` list to describe each table, selected columns, aliases, and join conditions. dbt then loops over that metadata to generate a single query with `LEFT JOIN`s:

- Orders are the driving table.
- Customers join on `customer_id`.
- Order items join on `order_id`.
- Products join through `order_item_id`/`product_id`.
- Stores join on `store_id`.
- Employees join on `store_id`.

The resulting `silver_obt` includes order measures such as `total_amount`, item measures such as `quantity`, `unit_price`, and `line_amount`, descriptive customer/product/store/employee attributes, source timestamps, active flags, and processing timestamps. This model is configured as a table in the `walmart_silver` schema.

### 4. Gold layer

The gold models are organized under `walmart_project/models/gold/`:

- `ephemeral/eph_customers.sql`, `eph_employees.sql`, `eph_orders.sql`, `eph_products.sql`, and `eph_stores.sql` derive distinct entity-shaped datasets from `walmart_silver.silver_obt`. Because the project configures `gold/ephemeral` as ephemeral, dbt inlines these transformations into downstream queries rather than persisting standalone tables.
- `fact/fact_orders.sql` projects the key order and line-item fields: `order_id`, `order_item_id`, `product_id`, `store_id`, `employee_id`, `customer_id`, `total_amount`, `quantity`, `unit_price`, and `line_amount`. The gold configuration places this model in the `walmart_gold` schema and materializes it as a table.

### 5. Orchestration

`airflow/dags/orchestrate.py` defines the `orchestrate` DAG. Its intended dependency chain is:

1. `ingest_cdc` — currently a placeholder task that returns a message.
2. `clean_target` — removes the dbt `target` and `logs` directories.
3. `source_freshness` — runs `dbt source freshness`.
4. `silver_technical` and `silver_technical_test` — builds and tests `silver_t`.
5. `silver_business` and `silver_business_test` — builds and tests `silver_obt_b`.
6. Gold model, snapshot/dimension, and fact tasks.

The Docker Compose environment runs Airflow with CeleryExecutor, Redis as the broker, and PostgreSQL as the metadata/result backend. The dbt project is mounted into containers at `/opt/airflow/walmart_project`.

## Repository layout

```text
.
├── airflow/
│   ├── dags/orchestrate.py       # Airflow DAG and dbt task dependencies
│   ├── Dockerfile                # Airflow image with dbt dependencies
│   ├── docker-compose.yaml       # Local Airflow, Redis, and PostgreSQL stack
│   ├── requirements.txt          # Airflow, Docker provider, dbt Core/BigQuery
│   └── walmart_project/           # Mounted copy of the dbt project for Airflow
├── src/data_project_dbt/
│   └── __init__.py               # Minimal Python package entry point
├── walmart_project/
│   ├── dbt_project.yml           # Main dbt project configuration
│   ├── models/source/             # BigQuery source declarations
│   ├── models/silver_t/           # Incremental technical silver models
│   ├── models/silver_obt_b/       # Silver OBT model
│   ├── models/gold/                # Ephemeral entities and fact model
│   ├── macros/custom_schema.sql   # Custom schema-name generation
│   ├── tests/test_dbt.sql         # Custom silver OBT key-integrity test
│   ├── analyses/                  # dbt analysis SQL location
│   ├── seeds/                     # dbt seed location; currently empty
│   └── snapshots/                 # dbt snapshot location
├── dbt_project.yml                # Root-level dbt path configuration
├── pyproject.toml                 # uv/Python package metadata and dependencies
└── uv.lock                        # Locked Python dependency graph
```

The `airflow/walmart_project/` directory mirrors the dbt project so that the containerized Airflow services can run dbt from the mounted path. When changing dbt code, keep the root project and the Airflow-mounted copy synchronized, or simplify the setup to use one canonical project directory.

## Technology stack

- **Python:** package metadata and Airflow DAG code; the project declares Python `>=3.14` in `pyproject.toml`.
- **SQL/Jinja:** dbt models, tests, macros, and source configuration.
- **dbt Core and dbt-bigquery:** transformation framework and BigQuery adapter.
- **Apache Airflow:** workflow orchestration and task dependency management.
- **Docker Compose:** local Airflow cluster with CeleryExecutor, Redis, and PostgreSQL.
- **Google BigQuery:** source, silver, and gold warehouse destination.
- **uv:** Python environment and dependency locking/build workflow.

## Prerequisites and configuration

1. Docker and Docker Compose.
2. Access to the configured Google Cloud project and BigQuery datasets.
3. A dbt BigQuery profile named `walmart_project`. The repository does not include a `profiles.yml`, so configure it in `~/.dbt/profiles.yml` or provide an equivalent secure runtime configuration.
4. Google Cloud authentication available to the environment that runs dbt. Do not commit service-account keys or other credentials.
5. At least the resources recommended by the compose file for local Airflow: approximately 4 GB memory, 2 CPUs, and 10 GB free disk space.

The checked-in source configuration contains a project identifier and datasets (`wallmart`, `walmart_silver`, and `walmart_gold`). Confirm these values match your BigQuery environment before running the project.

## Running dbt directly

From the repository root, use the actual dbt project directory:

```bash
cd walmart_project

# Install dependencies if using uv from the repository root
uv sync

# Confirm the BigQuery profile and adapter connection
dbt debug

# Check the registered source data
dbt source freshness

# Build all models and run tests
dbt build

# Or run the layers explicitly
dbt run --select silver_t
dbt test --select silver_t
dbt run --select silver_obt_b
dbt test --select silver_obt_b
dbt run --select gold
```

Use `dbt seed` only after adding seed files to `walmart_project/seeds/`. Use `dbt snapshot` only after adding valid snapshot definitions to `walmart_project/snapshots/`.

## Running Airflow locally

The compose configuration is intended for local development and uses default development credentials unless overridden. From the Airflow directory:

```bash
cd airflow

# Optional: create/update .env with a suitable AIRFLOW_UID and secure values
# Build the custom Airflow image with dbt dependencies
docker compose build

# Initialize the Airflow metadata database and admin user
docker compose up airflow-init

# Start the local Airflow services
docker compose up -d
```

Open the Airflow UI at <http://localhost:8081>. The compose file defaults to the `airflow` username and password for local setup; change these values for any shared environment. Logs and project files are mounted from the `airflow/` directory.

## Data quality

- `properties.yml` checks `products_t.product_id` and `stores_t.store_id` for nulls/duplicates and checks `products_t.product_name` for nulls.
- `tests/test_dbt.sql` is configured with warning severity and returns failing rows when any of the following OBT keys is null: `order_id`, `product_id`, `store_id`, `employee_id`, `order_item_id`, or `customer_id`.
- Run `dbt test` or `dbt build` to execute these checks.

## Known implementation notes

The repository is a useful end-to-end example, but review these items before production use:

- `airflow/dags/orchestrate.py` calls `dbt snapshort`; the dbt command is normally `dbt snapshot`.
- The DAG selects `gold/ephermeral`, while the directory is named `gold/ephemeral`.
- The `gold_facts` task uses `/opt/airflow_walmart_project` instead of the mounted `/opt/airflow/walmart_project` path.
- The DAG's `gold_dimension` task references snapshots, but the repository's `snapshots/` directory currently has no snapshot definition.
- `ingest_cdc` is a placeholder and does not ingest CDC data.
- `airflow/.env` and the compose defaults are development-oriented. Keep credentials and cloud authentication outside version control.
- There are two dbt project layouts/configurations: the root `dbt_project.yml` points at `walmart_project/...`, while `walmart_project/dbt_project.yml` is the conventional standalone project configuration. Use one canonical invocation path and keep the mirrored Airflow project synchronized.

## Useful dbt commands

```bash
dbt ls --resource-type model
dbt run --select silver_t+
dbt test --select silver_obt_b
dbt compile
dbt docs generate
dbt docs serve
dbt clean
```

## Further reading

- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [dbt source freshness](https://docs.getdbt.com/docs/build/sources#source-freshness)
- [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [Apache Airflow Docker Compose quickstart](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Google Cloud BigQuery](https://cloud.google.com/bigquery/docs)
