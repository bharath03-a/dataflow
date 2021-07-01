import argparse
import json
import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# Service account key path
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/bharathvelamala258/gcstobq/manifest-craft-316206-dda99930db77.json"

INPUT_SUBSCRIPTION = "projects/manifest-craft-316206/subscriptions/topic_1_stream-sub"
BIGQUERY_TABLE = "manifest-craft-316206:pubsub_bq.final_op"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"


class CustomParsing(beam.DoFn):

    def to_runner_api_parameter(self, unused_context):
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        parsed = json.loads(element.decode("utf-8"))
        parsed["timestamp"] = timestamp.to_rfc3339()
        yield parsed


def run():
    # Parsing arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription of the form "projects/manifest-craft-316206/subscriptions/topic_1_stream-sub."',
        default=INPUT_SUBSCRIPTION,
    )
    parser.add_argument(
        "--output_table", help="Output BigQuery Table", default=BIGQUERY_TABLE
    )
    parser.add_argument(
        "--output_schema",
        help="Output BigQuery Schema in text format",
        default=BIGQUERY_SCHEMA,
    )

    known_args, pipeline_args = parser.parse_known_args()

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()