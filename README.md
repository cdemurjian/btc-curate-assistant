# BTC Curate Assistant

BTC curation and data-ops toolkit for a growing set of BTC workflows.

## Main Commands

```bash
uv run btc curate
uv run btc fetch bioprojects-to-s3 s3://bucket/prefix
uv run --with pymongo btc curate mongo export
```

Legacy compatibility entrypoints still exist:

```bash
uv run curate
uv run rename --help
uv run pull-gbm-mongo --help
```

## Repo Layout

- `src/btc/` - Python package for BTC tooling
- `src/btc/curate/` - manifest curation workflows
- `src/btc/fetch/` - fetch command wrappers and future fetch logic
- `scripts/fetch/` - operational shell workflows for acquisition and staging
- `templates/manifests/` - tracked manifest workbook templates
- `manifests/bioprojects/` - tracked BioProject job manifests
- `docs/` - user and architecture docs
- `data/reference/` - gitignored local reference inputs and Mongo exports
- `data/workspace/` - gitignored runs, logs, staging, temp data

## Docs

- [Curate Overview](docs/curate/overview.md)
- [Fetch Overview](docs/fetch/overview.md)
- [NCBI Cloud Delivery](docs/fetch/ncbi-cloud-delivery.md)
- [Manual Smoke Tests](docs/architecture/manual-smoke-tests.md)
- [Repo Structure](docs/architecture/repo-structure.md)
- [Coding Agents](docs/architecture/coding-agents.md)
- [dash_curation venv (UV)](docs/curate/dash-curation-venv-uv.md)
