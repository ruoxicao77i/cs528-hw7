# HW3: Apache Beam + Cloud Dataflow

This homework processes the HTML files generated in HW2 (stored in a GCS bucket).

- `top_links.py`: top 5 files with the most incoming links and the most outgoing links
- `top_trigrams.py`: top 5 most frequent word trigrams
- `run_all.sh`: runs both programs locally (DirectRunner) and on Cloud Dataflow

Each program prints its total runtime when it finishes.

## Setup

1. Log in and choose the project:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   gcloud auth application-default login
   ```

2. Enable the APIs:
   ```bash
   gcloud services enable dataflow.googleapis.com compute.googleapis.com \
     storage.googleapis.com logging.googleapis.com
   ```

3. Install Apache Beam:
   ```bash
   pip install "apache-beam[gcp]"
   ```

## Run

1. Edit the top of `run_all.sh` and set `PROJECT`, `BUCKET`, and the input folder.
2. Run:
   ```bash
   bash run_all.sh
   ```

To run one program by itself:

```bash
# Local
python top_links.py --input "gs://YOUR_BUCKET/YOUR_DIR/*.html" --runner DirectRunner

# Dataflow
python top_links.py --input "gs://YOUR_BUCKET/YOUR_DIR/*.html" \
  --output gs://YOUR_BUCKET/hw3_output/top_links \
  --runner DataflowRunner --project YOUR_PROJECT_ID --region us-central1 \
  --temp_location gs://YOUR_BUCKET/tmp --staging_location gs://YOUR_BUCKET/staging \
  --job_name hw3-links
```

## Output

- Local runs print the results in the terminal.
- Dataflow runs write the results to the bucket. View them with:
  ```bash
  gsutil cat gs://YOUR_BUCKET/hw3_output/top_links.txt
  gsutil cat gs://YOUR_BUCKET/hw3_output/top_trigrams.txt
  ```
- Job status and the pipeline graph can be found in the GCP console under **Dataflow > Jobs**.