# Flink Batch ETL Job — Toll Plaza Data Consolidation

## Overview

This PyFlink batch job replaces the shell-based ETL pipeline (previously orchestrated by `ETL_toll_data.py` via Apache Airflow `BashOperator` tasks). It reads three source files containing toll plaza data in different formats, extracts relevant columns, consolidates them into a single dataset, and applies a transformation.

## Pipeline Steps

| Step | Original Shell Command | Flink Equivalent |
|------|----------------------|------------------|
| Extract CSV cols 1-4 | `cut -d"," -f1-4 vehicle-data.csv` | `extract_csv_fields()` |
| Extract TSV cols 5-7 | `cut -f5,6,7 \| tr "\t" "," \| tr -d "\r"` | `extract_tsv_fields()` |
| Extract fixed-width | `cut -c59- \| tr " " "," \| tr -d "\r"` | `extract_fixed_width_fields()` |
| Consolidate | `paste -d","` (row-by-row join) | Keyed join on Rowid |
| Transform | `awk '{$4=toupper($4); print}'` | `transform_vehicle_type()` |

## Output Schema

The final CSV has 9 columns (no header):

```
Rowid, Timestamp, Anonymized Vehicle number, Vehicle type (UPPERCASED),
Number of axles, Tollplaza id, Tollplaza code, Type of Payment code, Vehicle Code
```

## Prerequisites

- **Python 3.9+** (tested with 3.11, 3.12)
- **Java 11 or 17** (required by PyFlink runtime)
- **Apache Flink** (`pip install apache-flink>=1.18.0`)

## Installation

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install runtime dependencies
pip install -r requirements.txt

# (Optional) Install dev dependencies for testing
pip install -r requirements-dev.txt
```

## Running Locally (Standalone / Mini-Cluster)

PyFlink automatically starts a local mini-cluster when no external cluster is configured.

```bash
# Using default file names in current directory
python -m toll_etl.flink_batch_job \
    --vehicle-csv vehicle-data.csv \
    --tollplaza-tsv tollplaza-data.tsv \
    --payment-txt payment-data.txt \
    --output transformed_data.csv

# Or using environment variables
export TOLL_VEHICLE_CSV=/path/to/vehicle-data.csv
export TOLL_TOLLPLAZA_TSV=/path/to/tollplaza-data.tsv
export TOLL_PAYMENT_TXT=/path/to/payment-data.txt
export TOLL_OUTPUT_PATH=/path/to/output/transformed_data.csv
python -m toll_etl.flink_batch_job
```

### CLI Arguments

| Argument | Env Variable | Default | Description |
|----------|-------------|---------|-------------|
| `--vehicle-csv` | `TOLL_VEHICLE_CSV` | `vehicle-data.csv` | Path to CSV input |
| `--tollplaza-tsv` | `TOLL_TOLLPLAZA_TSV` | `tollplaza-data.tsv` | Path to TSV input |
| `--payment-txt` | `TOLL_PAYMENT_TXT` | `payment-data.txt` | Path to fixed-width input |
| `--output` | `TOLL_OUTPUT_PATH` | `transformed_data.csv` | Output file path |
| `--parallelism` | `TOLL_PARALLELISM` | `1` | Flink job parallelism |

## Submitting to a Flink Cluster

### Option 1: `flink run` (Flink CLI)

```bash
# Ensure FLINK_HOME is set and the cluster is running
$FLINK_HOME/bin/flink run \
    --python toll_etl/flink_batch_job.py \
    --pyFiles toll_etl/ \
    --parallelism 1 \
    -- --vehicle-csv /data/vehicle-data.csv \
       --tollplaza-tsv /data/tollplaza-data.tsv \
       --payment-txt /data/payment-data.txt \
       --output /data/output/transformed_data.csv
```

### Option 2: Session Cluster (Docker)

```bash
# Start a Flink session cluster
docker run -d --name flink-jobmanager \
    -p 8081:8081 \
    -e FLINK_PROPERTIES="jobmanager.rpc.address: localhost" \
    flink:1.18 jobmanager

docker run -d --name flink-taskmanager \
    -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" \
    --link flink-jobmanager \
    flink:1.18 taskmanager

# Submit the job
docker exec flink-jobmanager flink run \
    --python /path/to/toll_etl/flink_batch_job.py \
    --pyFiles /path/to/toll_etl/
```

### Option 3: YARN / Kubernetes

For production deployments on YARN or Kubernetes, package the application and submit:

```bash
# YARN
$FLINK_HOME/bin/flink run -m yarn-cluster \
    --python toll_etl/flink_batch_job.py \
    --pyFiles toll_etl/

# Kubernetes (native)
$FLINK_HOME/bin/flink run-application \
    --target kubernetes-application \
    --python toll_etl/flink_batch_job.py \
    --pyFiles toll_etl/
```

## Running Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Tests use small fixture files in `tests/fixtures/` and do NOT require PyFlink or Java.

## Architecture

```
toll_etl/
├── __init__.py          # Package marker
├── __main__.py          # Entry point for `python -m toll_etl`
├── config.py            # CLI/env-var configuration
├── transforms.py        # Pure extraction/transform functions (no Flink dependency)
└── flink_batch_job.py   # PyFlink DataStream wiring
```

The `transforms.py` module is intentionally free of any Flink imports so it can be:
- Unit tested without PyFlink installed
- Reused in a future Flink Table API / streaming unification (Ticket 3)
- Imported by other tools or scripts
