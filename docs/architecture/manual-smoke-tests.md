# Manual Smoke Tests

Use these commands to validate the reorganized repo before doing real work.

## Preflight

```bash
uv run btc --help
uv run btc curate --help
uv run btc fetch --help
aws sso login --profile curate
```

## Curate

Create a non-interactive plan:

```bash
uv run btc curate plan s3://example-bucket/example-prefix/
```

Start a new interactive curate run:

```bash
uv run btc curate
```

At the prompt, paste a real S3 prefix and confirm that a new folder appears under `data/workspace/curate/runs/`.

Resume an existing run:

```bash
uv run btc curate
```

At startup, choose one of the existing run folders and confirm that the plan is reloaded and manifests are rendered again into the same run directory.

Test rename help:

```bash
uv run btc curate rename --help
```

Test Mongo export help:

```bash
uv run --with pymongo btc curate mongo --help
uv run --with pymongo btc curate mongo export --limit 5
```

Confirm that subject and biospecimen CSVs are written under `data/reference/mongo/`.

## Fetch

Generate the tracked default manifest:

```bash
uv run btc fetch manifest generate
```

Dry-run a fetch from the tracked manifest:

```bash
DRY_RUN=1 uv run btc fetch from-manifest manifests/bioprojects/default.json
```

Dry-run a BioProject-to-S3 fetch:

```bash
DRY_RUN=1 uv run btc fetch bioprojects-to-s3 s3://your-bucket/your-prefix
```

Generate a one-project manifest directly from a BioProject accession:

```bash
uv run btc fetch bioproject PRJNA1368480 fastq s3://your-bucket/your-prefix/PRJNA1368480/
```

Dry-run the BioProject-first flow end to end:

```bash
DRY_RUN=1 uv run btc fetch bioproject PRJNA1368480 fastq s3://your-bucket/your-prefix/PRJNA1368480/ --fetch
```

Dry-run GSM concatenation:

```bash
DRY_RUN=1 uv run btc fetch gsm-cat
```

Test BAM validation help:

```bash
uv run btc fetch bam test --help
uv run btc fetch bam remap --help
```

If you have a local reference FASTA and an example BAM, run:

```bash
REFERENCE_FASTA=data/reference/fetch/UU_Cfam_GSD_1.0_ROSY.fa \
uv run btc fetch bam remap /path/to/input.bam
```

## New BioProject Fetch

Preferred path:

```bash
DRY_RUN=1 uv run btc fetch bioproject PRJNA1368480 fastq s3://your-bucket/your-prefix/PRJNA1368480/ --fetch
```

Manual path if you want to edit the manifest first:

```bash
cp manifests/bioprojects/default.json manifests/bioprojects/my-test.json
DRY_RUN=1 uv run btc fetch from-manifest manifests/bioprojects/my-test.json
```

Before a real run, verify:

- the BioProject accession
- the `data_type`
- the destination S3 prefix
- the run accession list

## What To Confirm

- new curate runs land in `data/workspace/curate/runs/`
- resume flow reuses an existing run folder
- templates are copied from `templates/manifests/`
- Mongo CSVs land in `data/reference/mongo/`
- fetch scripts stage under `data/workspace/fetch/`
- no workflow creates fresh state under legacy root folders like `runs/`, `files/`, or `work/`
