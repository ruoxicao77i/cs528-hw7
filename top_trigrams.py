import argparse
import re
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio


TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[a-z0-9']+")
def file_to_trigrams(readable_file:fileio.ReadableFile):
    """Turn one file into all of its word trigrams.
    
    Args:
            readable_file: A beam.io.fileio.ReadableFile produced by ReadMatches.
    """
    content = readable_file.read_utf8()
    text = TAG_RE.sub(" ", content).lower()
    words = WORD_RE.findall(text)
    for i in range(len(words)-2):
        yield " ".join(words[i:i+3])

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
        parsed = (
            p
            | "MatchFiles" >> fileio.MatchFiles(known_args.input)
            | "ReadMatches" >> fileio.ReadMatches()
            | "ToTrigrams" >> beam.FlatMap(file_to_trigrams)
        )

        top_trigrams = (
            parsed
            | "PairWithOne" >> beam.Map(lambda tri: (tri, 1))
            | "Count" >> beam.CombinePerKey(sum)
            | "Top5" >> beam.combiners.Top.Of(5, key=lambda kv:kv[1])
        )

        result = top_trigrams | "Format" >> beam.Map(
            lambda lst: "Top 5 trigrams:\n" + "\n".join(f"  '{t}': {c}" for t, c in lst))

        result | "Print" >> beam.Map(print)

        if known_args.output:
            result | "Write" >> beam.io.WriteToText(
                known_args.output, file_name_suffix=".txt", shard_name_template=""
            )
    elapsed = time.time() - start
    print(f"Runner: {options.get_all_options()['runner']}, total time: {elapsed:.1f} s")

if __name__ == "__main__":
    run()

