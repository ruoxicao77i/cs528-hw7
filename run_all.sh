#!/usr/bin/env bash
# Run both HW3 programs locally and on Dataflow.
set -e

PROJECT="tonal-plateau-508118-g0"
BUCKET="cs528-ruoxi-pagerank"
INPUT="gs://${BUCKET}/pages/*.html"

DATAFLOW="--runner DataflowRunner --project ${PROJECT} --region us-central1 \
  --temp_location gs://${BUCKET}/tmp --staging_location gs://${BUCKET}/staging"

# Local
python top_links.py    --input "${INPUT}" --runner apache_beam.runners.portability.fn_api_runner.FnApiRunner
python top_trigrams.py --input "${INPUT}" --runner apache_beam.runners.portability.fn_api_runner.FnApiRunner

# Dataflow
python top_links.py    --input "${INPUT}" --output "gs://${BUCKET}/hw7_output/top_links"    ${DATAFLOW} --job_name hw7-links
python top_trigrams.py --input "${INPUT}" --output "gs://${BUCKET}/hw7_output/top_trigrams" ${DATAFLOW} --job_name hw7-trigrams
