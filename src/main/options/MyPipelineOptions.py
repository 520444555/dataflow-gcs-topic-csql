import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
 

class MyPipelineOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--logging_mode',default='INFO')
    parser.add_argument('--subscription')
    parser.add_argument('--ivr_table_name')
    parser.add_argument('--ivr_error_table_name')
    parser.add_argument('--sql_instance')
    parser.add_argument('--username')
    parser.add_argument('--db_name')
    parser.add_argument('--topic_name')
