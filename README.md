# BTC Curate Assistant

CLI tooling for BTC metadata curation. The main command is:

```bash
uv run curate
```

## What It Does

This tool starts from an S3 path and builds a reproducible curation run under `runs/`.

At a high level, it:

- checks that AWS SSO is configured and logged in
- accepts either an `s3://...` path or an S3 HTTPS URL
- inventories the S3 prefix into a timestamped CSV with `file_path,size`
- filters common fake files such as `.DS_Store`, `._*`, directory markers, and blank keys
- asks the curation planning questions and saves the answers in `plan.json`
- copies the latest dated manifest templates from `templates/`
- fills the file and biospecimenfile manifests from the S3 inventory and saved plan values
- for GBM Sharma runs, proposes subject and biospecimen IDs from `files/gbm/GBM-master-list.xlsx`
- for White proteomics runs, derives per-file panels from the file paths and walks an interactive GBM tracker sample-selection flow
- checks proposed subject and biospecimen IDs against Mongo reference CSVs in `files/mongo/`

The generated run files live in `runs/YYMMDD_HHMMSS/`. The `runs/` and `files/` data folders are gitignored because they can contain project metadata or PHI-adjacent information.

## What You Need Set Up

Install or configure:

- `uv`
- AWS CLI
- an AWS SSO profile with access to the target S3 bucket
- a repo-local `.env`
- current BTC templates in `templates/`
- project support files under `files/`, for example `files/gbm/GBM-master-list.xlsx`

Create `.env` from the example:

```bash
cp .env.example .env
```

For local S3 inventory and manifest work, `.env` should include:

```bash
AWS_PROFILE=curate
AWS_REGION=us-east-1
OUTPUT_DIR=runs
FILES_DIR=files
TEMPLATES_DIR=templates
```

## VM Setup

On the BTC VM, get Mongo credentials from the BTC DS team and add them to `.env`:

```bash
MONGODB_URI="mongodb://<username>:<password>@btc-prod-docdb.cluster-cfkw4s6gw569.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsInsecure=true&tlsCAFile=global-bundle.pem&authSource=admin&retryWrites=false"
MONGODB_DATABASE=dash_production
```

DocumentDB TLS needs the AWS RDS trust bundle even when running on the BTC VM:

```bash
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
chmod 600 global-bundle.pem
```

Confirm the Mongo connection:

```bash
uv run --with pymongo pull-gbm-mongo databases
uv run --with pymongo pull-gbm-mongo list
```

Export the current subject and biospecimen references:

```bash
uv run --with pymongo pull-gbm-mongo export
```

This writes:

- `files/mongo/subject-YYMMDD.csv`
- `files/mongo/biospecimen-YYMMDD.csv`

`uv run curate` also performs this check automatically on the BTC VM. After AWS SSO succeeds, it checks whether today's Mongo CSVs exist. If not, it runs the export step when Mongo is configured. If Mongo is not configured or the export fails, it falls back to the latest cached `files/mongo/subject-*.csv` and `files/mongo/biospecimen-*.csv` when both exist.

VM detection uses the hostname by default. To force behavior:

```bash
BTC_CURATE_ASSISTANT_ON_VM=1   # force VM behavior
BTC_CURATE_ASSISTANT_ON_VM=0   # force local behavior
```

## How To Use It

Log in to AWS SSO if needed:

```bash
aws sso login --profile curate
```

Start the curation assistant:

```bash
uv run curate
```

For a new run, paste an S3 path when prompted. Both forms are accepted:

```text
s3://btc-gbm/staging/trial1a/sharma_lab/scrna/BTC_BCR_Pt_1_to_6/
https://btc-gbm.s3.us-east-1.amazonaws.com/staging/trial1a/sharma_lab/scrna/BTC_BCR_Pt_1_to_6/
```

The CLI will:

- create a new timestamped run folder
- write the S3 inventory CSV
- ask whether subjects or biospecimens need registration
- ask for teamlab and modality
- copy the needed templates
- ask manifest-level fields such as assay, study, lab, contact, panel, platform, and vendor
- render updated manifest workbooks in the run folder

To resume an existing run, choose one of the listed `runs/` folders when the CLI starts.

## Repo Map

- `src/btc_manifest/cli.py`: main `uv run curate` workflow
- `src/btc_manifest/aws.py`: AWS SSO checks and S3 inventory handling
- `src/btc_manifest/config.py`: `.env` configuration
- `src/btc_manifest/inventory.py`: inventory parsing and file metadata helpers
- `src/btc_manifest/manifests.py`: shared manifest rendering
- `src/btc_manifest/modalities/`: modality-specific mapping logic
- `src/btc_manifest/scripts/pull_gbm_mongo.py`: BTC VM Mongo export helper
- `templates/`: source `.xlsx` templates
- `files/`: gitignored project/reference files
- `runs/`: gitignored generated curation runs

## Developer Notes

The package entry points are defined in `pyproject.toml`:

```bash
uv run curate
uv run --with pymongo pull-gbm-mongo export
```

The code is importable from `src/btc_manifest/`, for example:

```python
from btc_manifest.aws import parse_s3_uri
from btc_manifest.inventory import read_inventory_rows
from btc_manifest.manifests import render_manifest_files
```
