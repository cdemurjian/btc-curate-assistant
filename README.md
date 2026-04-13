# BTC Curate Assistant

Command-line tooling for end-to-end metadata manifest curation for the BTC project.

The intended workflow starts from an AWS S3 path, pulls project metadata from MongoDB, uses local Excel templates from `templates/`, combines project-specific inputs from `files/`, and writes a reproducible curation run under `runs/`.

## Prerequisites

- `uv`
- AWS CLI with SSO configured and logged in for the target account/profile
- A `.env` file with MongoDB credentials and project defaults
- Excel templates in `templates/`
- Any project support files in `files/gbm`, `files/kras`, or `files/other`

Create a local env file from the example:

```bash
cp .env.example .env
```

At minimum for local S3 inventory work, set:

```bash
AWS_PROFILE=curate
AWS_REGION=us-east-1
OUTPUT_DIR=runs
FILES_DIR=files
TEMPLATES_DIR=templates
```

## Quick Start

```bash
uv sync
uv run curate
```

The CLI checks AWS SSO first. If the profile exists but your SSO token is expired, it exits and prints the login command to run:

```bash
aws sso login --profile curate
```

For now, the CLI checks AWS SSO, lets you start a new run or resume a run from `runs/`, prompts for an S3 path when starting a new run, runs `aws s3 ls <path> --recursive --profile <profile>`, and writes a timestamped CSV under `runs/`. The CSV has `file_path,size` headers, uses full `s3://bucket/key` paths, and filters common fake files such as `.DS_Store`, `._*`, `.` entries, and directory marker keys.

After S3 inventory, the CLI asks:

- whether subjects need registration (`Y`/`N` accepted)
- whether biospecimens need registration (`Y`/`N`/`M` accepted; skipped and set to `Yes` when subject registration is `Yes`)
- the BTC teamlab
- the custom modality (`no_modality-default` or `sharma`)

Those answers are saved in `plan.json`. The CLI then copies the latest dated matching templates into the run folder: file and biospecimenfile templates always, subject template only when subject registration is `Yes`, and biospecimen template when biospecimen registration is `Yes` or `Maybe`.

For manifest rendering, the CLI asks for one or more assays, study, lab, lab contact, lab first name, lab contact first name, panel, platform, and vendor. For `GBM`, study defaults to `GBM_Trial1`. The shared file manifest renderer fills the copied file manifest with selected assay(s), selected teamlab, `file_source`, `file_extension`, and `file_size` from the S3 inventory, and sets `data_level=1` for `fastq.gz`/`fq.gz` files. Multiple selected assays are currently written as a comma-separated value on every file row; per-file assay assignment still needs modality-specific logic. The biospecimenfile manifest renderer fills selected teamlab, study, and `file_source`; subject and biospecimen IDs are handled by modality-specific mapping and are currently blank for `sharma`.

## Repo Layout

- `cli.py`: legacy local wrapper; prefer `uv run curate`
- `src/btc_manifest/cli.py`: terminal prompts and workflow orchestration
- `src/btc_manifest/aws.py`: AWS SSO checks, S3 path normalization, and S3 inventory command handling
- `src/btc_manifest/config.py`: `.env` and environment configuration
- `src/btc_manifest/inventory.py`: inventory CSV parsing, fake-file filtering, file extensions, and data-level helpers
- `src/btc_manifest/manifests.py`: shared file and biospecimenfile manifest rendering
- `src/btc_manifest/mongo_exports.py`: VM detection and automatic Mongo reference export checks
- `src/btc_manifest/plans.py`: curation run plan creation, loading, and saving
- `src/btc_manifest/templates.py`: template discovery, copying, XLSX row replacement, and hint stripping
- `src/btc_manifest/modalities/`: modality-specific hooks, starting with `sharma`
- `src/btc_manifest/scripts/pull_gbm_mongo.py`: BTC VM helper for DocumentDB discovery and CSV export
- `templates/`: source `.xlsx` templates to fill
- `files/gbm/`: local GBM files used by the curation process
- `files/kras/`: local KRAS files used by the curation process
- `files/other/`: other local project files used by the curation process
- `files/mongo/`: exported subject and biospecimen reference CSVs, ignored by git
- `runs/`: generated curation outputs, ignored by git except for `.gitkeep`

## Current Pipeline

1. Parse and validate the input S3 URI.
2. Resolve runtime configuration from `.env` and environment variables.
3. Verify the AWS CLI profile exists and the SSO session is active.
4. On the BTC VM, ensure today's `files/mongo/subject-YYMMDD.csv` and `files/mongo/biospecimen-YYMMDD.csv` exist, exporting them from DocumentDB if needed.
5. Start or resume a run under `runs/`.
6. Discover S3 objects under the project prefix and write the timestamped inventory CSV.
7. Ask curation questions and save them in `plan.json`.
8. Copy the latest dated required templates from `templates/`.
9. Render file and biospecimenfile manifests from the inventory CSV and saved plan variables.

## Development

```bash
uv run curate --help
```

## Importable Package

The command-line tools are wrappers around importable package modules under `src/btc_manifest/`.
Examples:

```python
from btc_manifest.aws import parse_s3_uri
from btc_manifest.inventory import read_inventory_rows
from btc_manifest.manifests import render_file_manifest
from btc_manifest.plans import build_plan
from btc_manifest.modalities.sharma import biospecimenfile_ids_for_row
```

The CLI entrypoints are defined in `pyproject.toml`:

```bash
uv run curate
uv run --with pymongo pull-gbm-mongo export
```

## VM Mongo Pull

On the BTC VM, use the standalone Mongo export helper to inspect collections and export local GBM reference files.

DocumentDB TLS needs the AWS RDS trust bundle even when you are already on the VM:

```bash
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
chmod 600 global-bundle.pem
```

Set `.env` with the DocumentDB URI and database. Example:

```bash
MONGODB_URI="mongodb://<username>:<password>@btc-prod-docdb.cluster-cfkw4s6gw569.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsInsecure=true&tlsCAFile=global-bundle.pem&authSource=admin&retryWrites=false"
MONGODB_DATABASE=dash_production
```

First confirm the available databases:

```bash
uv run --with pymongo pull-gbm-mongo databases
```

Then list the collections in the selected database:

```bash
uv run --with pymongo pull-gbm-mongo list
```

For a more verbose discovery pass:

```bash
uv run --with pymongo pull-gbm-mongo discover
```

Export defaults to the `subject` and `biospecimen` collections:

```bash
uv run --with pymongo pull-gbm-mongo export
```

If the collection names need to be overridden:

```bash
uv run --with pymongo pull-gbm-mongo export --subject-collection subject --biospecimen-collection biospecimen
```

This writes:

- `files/mongo/subject-YYMMDD.csv`
- `files/mongo/biospecimen-YYMMDD.csv`

Those files are under `files/`, so they are ignored by git.

When `uv run curate` runs on a BTC VM, it checks for today's `files/mongo/subject-YYMMDD.csv` and `files/mongo/biospecimen-YYMMDD.csv` immediately after AWS SSO succeeds. If either file is missing, it automatically runs:

```bash
uv run --with pymongo pull-gbm-mongo export
```

VM detection uses the hostname by default. To force behavior, set:

```bash
BTC_CURATE_ASSISTANT_ON_VM=1   # force VM behavior
BTC_CURATE_ASSISTANT_ON_VM=0   # force local behavior
```
