import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

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
    parser.add_argument('--output',
                        dest='output',
                        help='Outputfilename',
                        default='data/departs')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_rows = p | "Read from TXT">>beam.io.ReadFromText(known_args.input)
        #ans = {}
        filter_accounts = (
        input_rows
        | beam.Map(lambda record: record.split(','))
            # |beam.Filter(filtering)
        | beam.Filter(lambda record: record[3] == 'Accounts')
        )
        
        #####################################
        ## Get Distinct Employees belonging to Accounts Department
        unique_employees = (
            filter_accounts 
            | beam.Map(lambda record:(record[1],1))
            | beam.CombinePerKey(sum)
            | beam.Map(lambda record:record[0])
            )

        l1 = len(unique_employees)
        p1=(unique_employees |"Write to Output">>beam.io.WriteToText(known_args.output))
        #####################################################

        ######################  YOUR CODE HERE ##############
        ## Obtain Number of Employees belonging to each department and save in a separate output file
        # since for accounts it is already given
        #we just need to update the dictionary i have created
        
        #for HR
        filter_HR = (
        input_rows
        | beam.Map(lambda record: record.split(','))
            # |beam.Filter(filtering)
        | beam.Filter(lambda record: record[3] == 'HR')
        )
        ## Get Distinct Employees belonging to HR Department
        unique_employees_HR = (
            filter_HR 
            | beam.Map(lambda record:(record[1],1))
            | beam.CombinePerKey(sum)
            | beam.Map(lambda record:record[0])
            )
        l2 = len(unique_employees_HR)

        #for HR
        filter_finance = (
        input_rows
        | beam.Map(lambda record: record.split(','))
            # |beam.Filter(filtering)
        | beam.Filter(lambda record: record[3] == 'Finance')
        )
        ## Get Distinct Employees belonging to HR Department
        unique_employees_finance = (
            filter_HR 
            | beam.Map(lambda record:(record[1],1))
            | beam.CombinePerKey(sum)
            | beam.Map(lambda record:record[0])
            )
        l3 = len(unique_employees_finance)
        ans = { 'Accounts' : l1, 'HR' : l2, 'Finance' : l3}
        p2 = ( ans | "write to a file">>beam.io.WriteToText(known_args.departs))
        ######################################################

        
        
        p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()