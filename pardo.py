import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class SplitRow(beam.DoFn):
  
  def process(self, element):
    # return type -> list
    yield  element.split(',')

class FilterAccountsEmployee(beam.DoFn):
  
  def process(self, element):
    if element[3] == 'Accounts':
      yield element 
    
class PairEmployees(beam.DoFn):
  
  def process(self, element):
    yield (element[3]+","+element[1], 1)   
  
class Counting(beam.DoFn):
  
  def process(self, element):
    # yield type -> list         # [Marco, Accounts  [1,1,1,1....] , Rebekah, Accounts [1,1,1,1,....] ]
    yield (element[1], 1)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='File to read',
                        default='demo_data.txt')
    parser.add_argument('--output',
                        dest='output',
                        help='Outputfilename',
                        default='data/output')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_rows = p | "Read from TXT">>beam.io.ReadFromText(known_args.input)
        
        filter_accounts = (
        input_rows
        | beam.ParDo(SplitRow())
            # |beam.Filter(filtering)
        | beam.ParDo(FilterAccountsEmployee())
        )
        

        #####################################
        ## Get Distinct Employees belonging to Accounts Department
        unique_employees = (
            filter_accounts 
            | beam.ParDo(Counting())
            | beam.CombinePerKey(sum)
            | beam.Map(lambda record:record[0])
            )
        p1=(unique_employees |"Write to Output">>beam.io.WriteToText(known_args.output))
        #####################################################
        
        ######################  YOUR CODE HERE ##############
        ## Implement CombinePerKey as a ParDo Object
        unique_emp = (
            filter_accounts 
            | beam.ParDo(Counting())
            | beam.ParDo(beam.CombinePerKey(sum))
            | beam.Map(lambda record:record[0])
            )
        ######################################################

        
        
        p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()