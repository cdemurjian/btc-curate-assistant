# NCBI Cloud Delivery

Use this workflow when SRA Toolkit does not expose the original submitted files and NCBI requires Cloud Data Delivery instead.

For the BTC cases discussed so far, this is the path for getting the original submitted BAMs for projects like `PRJNA1367604` and `PRJNA1374203`.

## When To Use This

Use Cloud Data Delivery when:

- SRA Toolkit does not provide the original submitted files
- NCBI shows the real files are available through the Cloud Data Delivery workflow
- you want NCBI to copy directly into your AWS bucket instead of pulling from a VM

This is a bucket-to-bucket delivery initiated by NCBI, not a local `aws s3 cp` workflow.

## Requirements

- a MyNCBI login
- an AWS bucket you control
- the bucket must be in `us-east-1`
- the destination bucket name for BTC is `btc-osteo`

You can deliver into a subfolder or prefix inside the bucket, for example:

- `staging/PRJNA1367604`
- `staging/PRJNA1374203`

Do not put a trailing slash on the bucket name itself.

## Owner Workflow

1. Open the SRA Run Selector:
   `https://www.ncbi.nlm.nih.gov/Traces/study/`
2. Paste the BioProject accession, for example `PRJNA1367604`.
3. Wait for the run table to load.
4. Select the runs to deliver.
   If you want the whole project, use the top checkbox to select all visible runs.
5. Click `Deliver Data`.
6. Log into MyNCBI if prompted.
7. In the delivery workflow, choose `AWS` as the cloud provider.
8. Select or register the destination bucket.
   For BTC, use `btc-osteo`.
9. Set the destination prefix.
   For example:
   `staging/PRJNA1367604`
10. Choose the file type to deliver.
    Select the original submitted BAM files.
11. Review the summary page.
    Confirm file counts, file sizes, bucket, and destination prefix.
12. Submit the delivery request.
13. Repeat for the next project, for example `PRJNA1374203`.

## Bucket Permission Step

This is the part the bucket owner needs to handle.

NCBI’s documentation says the Cloud Data Delivery workflow helps create and attach the relevant permissions to your bucket. In practice, that means:

- you enter or register the AWS bucket in the delivery flow
- NCBI presents a bucket-registration or authorization step
- the bucket owner completes that step so NCBI’s delivery service can write objects into the target bucket or prefix

Do not assume the exact AWS screen ahead of time. The important thing is to complete whatever bucket access step NCBI presents during bucket registration.

## What To Double-Check

- bucket is `btc-osteo`
- AWS region is `us-east-1`
- destination prefix is correct for the specific project
- selected file type is the original submitted BAM, not the normalized SRA representation

## What To Expect After Submission

- NCBI runs the transfer on their side
- delivery may complete quickly, but NCBI says it can take up to 48 hours
- NCBI sends email when delivery completes

## Example BTC Entries

For `PRJNA1367604`:

- bucket: `btc-osteo`
- prefix: `staging/PRJNA1367604`
- file type: original submitted BAM

For `PRJNA1374203`:

- bucket: `btc-osteo`
- prefix: `staging/PRJNA1374203`
- file type: original submitted BAM

## References

- SRA Run Selector: `https://www.ncbi.nlm.nih.gov/Traces/study/`
- NCBI Cloud Data Delivery docs: `https://www.ncbi.nlm.nih.gov/sra/docs/data-delivery/`
