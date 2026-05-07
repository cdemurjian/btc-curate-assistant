# Fetch Overview

The fetch area holds BTC acquisition and staging workflows.

Today the user-facing entrypoint is the umbrella CLI, which dispatches to the shell workflows under `scripts/fetch/`.

## Main Commands

```bash
uv run btc fetch bioproject PRJNA1368480 fastq s3://bucket/prefix/PRJNA1368480/
uv run btc fetch bioprojects-to-s3 s3://bucket/prefix
uv run btc fetch from-manifest manifests/bioprojects/default.json
uv run btc fetch manifest generate
uv run btc fetch s3-prefixes create
uv run btc fetch gsm-cat
uv run btc fetch bam test manifests/bioprojects/default.json
uv run btc fetch bam remap /path/to/input.bam
```

Most fetch workflows support `DRY_RUN=1` and should be exercised that way first.

## Layout

- `scripts/fetch/` - operational shell workflows
- `manifests/bioprojects/` - tracked BioProject manifests
- `data/workspace/fetch/` - gitignored staging, logs, state, temp data, local tool cache
- `data/reference/fetch/` - gitignored large references and fetch-specific local inputs
- `docs/fetch/ncbi-cloud-delivery.md` - owner workflow for NCBI Cloud Data Delivery into AWS

## Included Workflows

- `fetch_bioprojects_to_s3.sh` - pull configured BioProjects and upload one run at a time
- `btc fetch bioproject ...` - build a single-project manifest directly from a BioProject accession
- `fetch_from_manifest.sh` - fetch from a tracked manifest file
- `generate_bioproject_manifest.sh` - generate a BioProject manifest JSON
- `create_s3_bioproject_prefixes.sh` - create destination prefixes
- `cat_gsm_fastqs.sh` - concatenate SRR fastqs into GSM outputs
- `test_bam.sh` - sample BAM validation flow
- `remap_bam_to_reference.sh` - BAM validation/remap workflow

The fetch scripts now default to workspace paths under `data/workspace/fetch/` instead of writing next to source files.

## NCBI Cloud Delivery

If SRA Toolkit does not expose the original submitted files, use the NCBI Cloud Data Delivery workflow instead of the local fetch scripts:

- [NCBI Cloud Delivery](ncbi-cloud-delivery.md)

That guide covers the bucket-owner flow, the `btc-osteo` bucket, the `us-east-1` region requirement, and the permission/registration step NCBI asks the owner to complete.

## BioProject-First Flow

To start from a new BioProject accession, generate a single-project manifest directly:

```bash
uv run btc fetch bioproject PRJNA1368480 fastq s3://bucket/prefix/PRJNA1368480/
```

To generate the manifest and immediately hand it off to the existing fetch runner:

```bash
DRY_RUN=1 uv run btc fetch bioproject PRJNA1368480 fastq s3://bucket/prefix/PRJNA1368480/ --fetch
```

This writes `manifests/bioprojects/prjna1368480.json` by default unless `--manifest-out` is provided.
