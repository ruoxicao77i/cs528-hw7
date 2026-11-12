import argparse
import re
import os
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio


# Matches href="12.html" or HREF='12.html', case sensitive
_HREF = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

def parse_links(readable_file:fileio.ReadableFile):
    """Extract the file name and all outgoing links from this file.

    Args:
        readable_file: A beam.io.fileio.ReadableFile produced by ReadMatches.

    Returns:
        A tuple (src, targets), where src is the filename and targets is a list of linked file names.
    """
    src = os.path.basename(readable_file.metadata.path)
    content = readable_file.read_utf8()
    targets = [os.path.basename(t) for t in _HREF.findall(content)]
    return src, targets

def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="e.g. gs://bucket/dir/*.html")
    parser.add_argument("--output", default=None, help="optional gs://prefix for results")
    return parser.parse_known_args(argv)

def run(argv=None):
    known_args, pipeline_args = parse_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)
    start = time.time()
    with beam.Pipeline(options=options) as p:
        # Step 1: read every file in the bucket, keeping the file name
        parsed = (
            p
            | "MatchFiles" >> fileio.MatchFiles(known_args.input)
            | "ReadMatches" >> fileio.ReadMatches()
            | "ParseLinks" >> beam.Map(parse_links)
        )

        # Step 2: outgoing links = number of links inside each file
        top_out = (
            parsed
            | "OutCount" >> beam.Map(lambda kv: (kv[0], len(kv[1])))
            | "TopOut" >> beam.combiners.Top.Of(5, key=lambda kv: kv[1])
        )

        # Step 3: incoming links
        top_in = (
            parsed
            | "EmitEdges" >> beam.FlatMap(lambda kv: [(t, 1) for t in kv[1]])
            | "InCount" >> beam.CombinePerKey(sum)
            | "TopIn" >> beam.combiners.Top.Of(5, key=lambda kv: kv[1])
        )

        # Step 4: format results
        fmt_out = top_out | "FmtOut" >> beam.Map(
            lambda lst: "Top 5 outgoing:\n" + "\n".join(f"  {f}: {c}" for f, c in lst)
        )
        fmt_in = top_in | "FmtIn" >> beam.Map(
            lambda lst: "Top 5 incoming:\n" + "\n".join(f"  {f}: {c}" for f, c in lst)
        )
        results = (fmt_in, fmt_out) | "Merge" >> beam.Flatten()
        results | "Print" >> beam.Map(print)

        if known_args.output:
            results | "Write" >> beam.io.WriteToText(
                known_args.output, file_name_suffix=".txt", shard_name_template=""
            )
    elapsed = time.time() - start
    print(f"Runner: {options.get_all_options()['runner']}, total time: {elapsed:.1f} s")

if __name__ == "__main__":
    run()

