import logging
import re
import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from google.cloud import pubsub_v1
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/bharathvelamala258/gcstobq/manifest-craft-316206-dda99930db77.json"

class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    publisher = pubsub_v1.PublisherClient()
    topic_id = "projects/manifest-craft-316206/topics/topic_1_stream"
    topic_path = publisher.topic_path('manifest-craft-316206', 'topic_1_stream')

    data=element
    data = data.encode("utf-8")
    future = publisher.publish(topic_id, data)

    print(future.result())
    print (f"Published messages to {topic_path}.")



def run(argv=None, save_main_session=True):
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | 'Read' >> ReadFromText('gs://data-flow-01/full_history/test.txt')

    counts = (
        lines
        | 'To pubsub' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


