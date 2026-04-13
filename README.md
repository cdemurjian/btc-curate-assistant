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

## Quick Start

```bash
uv sync
uv run cli.py
```

During local development, this also works:

```bash
uv run cli.py
```

For now, the CLI checks AWS SSO, lets you start a new run or resume a run from `runs/`, prompts for an S3 path when starting a new run, runs `aws s3 ls <path> --recursive --profile <profile>`, and writes a timestamped CSV under `runs/`. The CSV has `file_path,size` headers, uses full `s3://bucket/key` paths, and filters common fake files such as `.DS_Store`, `._*`, `.` entries, and directory marker keys.

After S3 inventory, the CLI asks:

- whether subjects need registration (`Y`/`N` accepted)
- whether biospecimens need registration (`Y`/`N`/`M` accepted; skipped and set to `Yes` when subject registration is `Yes`)
- the BTC teamlab
- the custom modality (`no_modality-default` or `scRNA-sharma`)

Those answers are saved in `plan.json`. The CLI then copies the latest dated matching templates into the run folder: file and biospecimenfile templates always, subject template only when subject registration is `Yes`, and biospecimen template when biospecimen registration is `Yes` or `Maybe`.

For manifest rendering, the CLI asks for one or more assays, study, lab, lab contact, lab first name, lab contact first name, panel, platform, and vendor. For `scRNA-sharma`, assay defaults to `scRNA`; for `GBM`, study defaults to `GBM_Trial1`. The shared file manifest renderer fills the copied file manifest with selected assay(s), selected teamlab, `file_source`, `file_extension`, and `file_size` from the S3 inventory, and sets `data_level=1` for `fastq.gz`/`fq.gz` files. The biospecimenfile manifest renderer fills selected teamlab, study, and `file_source`; subject and biospecimen IDs are handled by modality-specific mapping and are currently blank for `scRNA-sharma`.

## Repo Layout

- `src/btc_manifest/cli.py`: terminal entrypoint, run planning, and lightweight local checks
- `src/btc_manifest/config.py`: `.env` and environment configuration
- `templates/`: source `.xlsx` templates to fill
- `files/gbm/`: local GBM files used by the curation process
- `files/kras/`: local KRAS files used by the curation process
- `files/other/`: other local project files used by the curation process
- `runs/`: generated curation outputs, ignored by git except for `.gitkeep`

## Initial Pipeline

1. Parse and validate the input S3 URI.
2. Resolve runtime configuration from `.env` and environment variables.
3. Verify AWS identity/profile and discover S3 objects under the project prefix.
4. Pull project metadata from MongoDB using identifiers derived from S3 and/or user input.
5. Resolve the relevant `.xlsx` template workbook from `templates/`.
6. Resolve project support files from `files/`.
7. Map metadata fields to workbook cells/tables.
8. Write completed workbooks and audit files to `runs/<timestamp>/`.
9. Optionally upload curated outputs back to S3 after review.

## Development

```bash
uv run cli.py --help
```

## VM Mongo Pull

On the BTC VM, use the standalone Mongo export helper to inspect collections and export local GBM reference files:

```bash
uv run --with pymongo scripts/pull_gbm_mongo.py discover
```

After choosing the right database and collections:

```bash
uv run --with pymongo scripts/pull_gbm_mongo.py export
```

This writes:

- `files/mongo/subject-YYMMDD.csv`
- `files/mongo/biospecimen-YYMMDD.csv`

Those files are under `files/`, so they are ignored by git.
