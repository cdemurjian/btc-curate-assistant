# Curate Overview

`uv run btc curate` is the manifest-curation workflow for BTC.

It starts from an S3 path, inventories the prefix, asks the curation planning questions, copies the latest manifest templates, and renders workbook outputs into `data/workspace/curate/runs/`.

## Required Local Inputs

- `.env`
- AWS CLI with SSO configured
- templates in `templates/manifests/`
- local reference files in `data/reference/`
- when needed, Mongo exports in `data/reference/mongo/`

Example `.env` values:

```bash
AWS_PROFILE=curate
AWS_REGION=us-east-1
OUTPUT_DIR=data/workspace/curate/runs
FILES_DIR=data/reference
TEMPLATES_DIR=templates/manifests
```

## Main Commands

```bash
uv run btc curate
uv run btc curate plan s3://bucket/prefix/
uv run btc curate rename s3://bucket/prefix/ --apply
uv run --with pymongo btc curate mongo export
```

Legacy compatibility commands remain available:

```bash
uv run curate
uv run rename
uv run pull-gbm-mongo
```

## Output And Reference Paths

- curation runs: `data/workspace/curate/runs/<timestamp>/`
- curated outputs: `data/workspace/curate/curated/`
- reference inputs: `data/reference/`
- Mongo CSV cache: `data/reference/mongo/`

## Notes

- The default interactive entrypoint is `uv run btc curate`.
- Mongo export requires `pymongo`, so use `uv run --with pymongo btc curate mongo export` unless your environment already has it installed.

## Code Map

- `src/btc/cli.py` - umbrella CLI
- `src/btc/curate/cli.py` - curate workflow
- `src/btc/common/aws.py` - AWS helpers
- `src/btc/common/config.py` - `.env` loading and runtime paths
- `src/btc/curate/modalities/` - modality-specific mapping logic
- `src/btc/curate/scripts/pull_gbm_mongo.py` - Mongo export helper
- `src/btc/curate/scripts/rename_s3_parentheses.py` - S3 rename workflow
