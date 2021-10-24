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
# For My MacBook
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/prabha/GitHub/chicago_crime/beam/chicago-crime-batch-processing.json"

# For my windows Laptop
#os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"C:\Users\arivalagan.prabhakar\Documents\Github\chicago_crime\beam\data_pipeline\batch\chicago-crime-batch-processing.json"
#bq_client = bigquery.Client()
#table_schema= 'id:numeric,case_number:string,date:datetime,block:string,iucr:string,primary_type:string,description:string,location_description:string,arrest:boolean,domestic:boolean,beat:string,district:string,ward:string,community_area:string,fbi_code:string,x_coordinate:numeric,y_coordinate:numeric,year:numeric,updated_on:datetime,latitude:float,longitude:float,location:string'
#table_schema = bq_client.schema_from_json('table_schema.json')
# parser = argparse.ArgumentParser()
# path_args,pipeline_args = parser.parse_known_args()



class chicago_crime_options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls,parser):
        parser.add_argument('--bigquery_table',type=str,help="BQ table to store the results")

class fetch_data(beam.DoFn):
    def process(self,url):
        updated_last_n_days = 3
        client = Socrata(url, app_token="Ttz4HIh52J3g53HKTYKMNxu4M")

        # Get all the updates in the last week.
        updated_on_filter = "updated_on >= '"+datetime.strftime(datetime.today().date()-timedelta(days = updated_last_n_days),'%Y-%m-%d')+"T00:00:00.000'"

        crimes = list(client.get_all("ijzp-q8t2",content_type='json',where = updated_on_filter,))
        
        
        logging.info("Number of records ",len(crimes))
        
        return crimes

class data_type_conversion(beam.DoFn):
    def process(self,row):
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
            #row['location']={}
        else:
            row['location']={}
         
        row['db_updated'] = datetime.now()
        

        return [row]
    

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    input_options = options.view_as(chicago_crime_options)
    
    data    =   (p 
                | 'URL ' >> beam.Create(['data.cityofchicago.org'])
                | 'Call API' >> beam.ParDo(fetch_data())
    )
    
    
    write_to_gbq = (
        data 
        | "Data Type Conversion" >> beam.ParDo(data_type_conversion())
       # | "Write to Text" >> beam.io.WriteToText("output")
        | "Write to GBQ Table" >> beam.io.WriteToBigQuery(input_options.bigquery_table,
        #                                                 schema=table_schema,
                                                         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                         custom_gcs_temp_location='gs://chicago-crime3-batch/tmp'
                                                         )
    )