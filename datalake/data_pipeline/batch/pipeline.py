from sodapy import Socrata
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from google.cloud import bigquery
from datetime import datetime,timedelta
import os
import json
import argparse

import logging
logging.info(" Pipeline Started...")

# Service Account Key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/prabha/GitHub/chicago_crime/beam/chicago-crime-batch-processing.json"



class chicago_crime_options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls,parser):
        parser.add_argument('--bigquery_table',type=str,help="BQ table to store the results")

class fetch_data(beam.DoFn):
    def process(self,url):
        """
        Pulls the data using API call , it pulls the records that are updated in the last 3 days.
        """
        updated_last_n_days = 3
        client = Socrata(url, app_token="Ttz4HIh52J3g53HKTYKMNxu4M")

        # Get all the updates in the last week.
        updated_on_filter = "updated_on >= '"+datetime.strftime(datetime.today().date()-timedelta(days = updated_last_n_days),'%Y-%m-%d')+"T00:00:00.000'"

        crimes = list(client.get_all("ijzp-q8t2",content_type='json',where = updated_on_filter,))
        
        
        logging.info("Number of records ",len(crimes))
        
        return crimes

class data_type_conversion(beam.DoFn):
    def process(self,row):
        """
        Converts the datatype for numeric columns
        """
        int_type = ['id','x_coordinate','y_coordinate']
        float_type = ['latitude','longitude']

    
        for col in int_type:
            if col in row:
                row[col] = int(row[col])
            else:
                row[col] = 0
            
        for col in float_type:
            if col in row:
                row[col] = float(row[col])
            else:
                row[col] = 0.0
    
        if 'location' in row:
            row['location']['human_address'] = json.loads(row['location']['human_address'])
        else:
            row['location']={}
         
        row['db_updated'] = datetime.now()
        
        return [row]
    

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    input_options = options.view_as(chicago_crime_options)
    
    # Fetching the data
    data    =   (p 
                | 'URL ' >> beam.Create(['data.cityofchicago.org'])
                | 'Call API' >> beam.ParDo(fetch_data())
    )
    
    # Converting the datatype and writing to BigQuery
    write_to_gbq = (
        data 
        | "Data Type Conversion" >> beam.ParDo(data_type_conversion())
        | "Write to GBQ Table" >> beam.io.WriteToBigQuery(input_options.bigquery_table,
                                                         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                         custom_gcs_temp_location='gs://chicago-crime3-batch/tmp'
                                                         )
    )