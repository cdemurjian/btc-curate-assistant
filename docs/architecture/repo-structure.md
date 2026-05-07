# Repo Structure

This repo is organized by BTC tool family rather than by implementation language.

## Principles

- package code under `src/btc/`
- operator shell workflows under `scripts/`
- tracked templates and manifests near the root
- mutable local state under `data/workspace/`
- mutable local reference inputs under `data/reference/`
- docs split by domain under `docs/`

## Domain Split

- `btc curate` covers metadata curation and manifest generation
- `btc fetch` covers acquisition, staging, and validation workflows

This layout is intended to absorb additional BTC curation tools without adding more top-level one-off scripts and ad hoc work directories.
