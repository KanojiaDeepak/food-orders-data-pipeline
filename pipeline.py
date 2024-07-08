import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import re

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process')
parser.add_argument('--dataset',
                      dest='dataset',
                      required=True,
                      help='Bigquery dataset')
parser.add_argument('--table',
                      dest='table',
                      required=True,
                      help='Bigquery table')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
#project_id = path_args.project
dataset_id = path_args.dataset
table_id = path_args.table

options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options = options)

def print_rows(x):
    print(x)
    print("---------------------")


def remove_colons_and_update_datetime(row):
    record=row.split(',')
    item=record[4]
    if item[-1]==':':
        record[4]=item[:-1]
    datetime=record[1]+' '+record[2]
    updated_record=[record[0],datetime]
    for i in range(3,11):
        updated_record.append(record[i])
    return ','.join(updated_record)

def remove_special_characters(row):
    record=row.split(',')
    updated_record=[]
    for col in record:
        updated_record.append(re.sub(r'[?%&]', '', col))
    return ','.join(updated_record)

def to_json(csv_str):
    fields = csv_str.split(',')
    
    json_str = {"customer_id":fields[0],
                 "datetime": fields[1],
                 "order_id": fields[2],
                 "items": fields[3],
                 "amount": fields[4],
                 "mode": fields[5],
                 "restaurant": fields[6],
                 "status": fields[7],
                 "ratings": fields[8],
                 "feedback": fields[9]
                 }

    return json_str


cleaned_data=(
    p
    | beam.io.ReadFromText(inputs_pattern,skip_header_lines=1)
    | 'Remove colons and merge date and time columns into single column' >> beam.Map(remove_colons_and_update_datetime)
    | 'Lower the alphabets' >> beam.Map(lambda row: row.lower())
    | 'Remove special characters' >> beam.Map(remove_special_characters)
)

write_to_bq=(
	cleaned_data
    | beam.Map(to_json)
    | 'Write data to BigQuery' >> beam.io.WriteToBigQuery(
        table=table_id,
        dataset=dataset_id,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}},
        schema='SCHEMA_AUTODETECT'
    )
)

p.run()

#Remove colons from items column and join date and time into one single column
#Lower alphabets
#Remove special characters

#Filter on basis of order status-delivered,other
#write to different bq tables on basis of order status

#Create a view that will only show current date's data