# BTC Manfiest

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
uv run btc-manfiest
```

During local development, this also works:

```bash
uv run cli.py
```

For now, the CLI checks AWS SSO, prompts for an S3 path, runs a recursive `aws s3 ls ... | awk ...` inventory command, and writes a timestamped run folder under `runs/`.

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
uv run btc-manfiest --help
```
