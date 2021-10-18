from sodapy import Socrata
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from datetime import datetime,timedelta
import os
import json
import argparse

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/prabha/GitHub/chicago_crime/beam/chicago-crime-batch-processing.json"

table_schema= 'id:numeric,case_number:string,date:datetime,block:string,iucr:string,primary_type:string,description:string,location_description:string,arrest:boolean,domestic:boolean,beat:string,district:string,ward:string,community_area:string,fbi_code:string,x_coordinate:numeric,y_coordinate:numeric,year:numeric,updated_on:datetime,latitude:float,longitude:float,location:string'

parser = argparse.ArgumentParser()
path_args,pipeline_args = parser.parse_known_args()
options = PipelineOptions(pipeline_args)


class fetch_data(beam.DoFn):
    def process(self,url):
        from datetime import datetime,timedelta
        from sodapy import Socrata
        updated_last_n_days = 1
        client = Socrata(url, app_token="Ttz4HIh52J3g53HKTYKMNxu4M")

        # Get all the updates in the last week.
        updated_on_filter = "updated_on >= '"+datetime.strftime(datetime.today().date()-timedelta(days = updated_last_n_days),'%Y-%m-%d')+"T00:00:00.000'"

        crimes = list(client.get_all("ijzp-q8t2",content_type='json',where = updated_on_filter,))
        
        crimes = crimes[1:] # remove the header row
        
        return crimes

    
def data_type_conversion(row):
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
        row['location'] = json.dumps(row['location'])
    else:
        row['location']=""

    return row
    
    

p1 = beam.Pipeline(options=options)

cleaned_data = (
    p1
    | beam.Create(['data.cityofchicago.org'])
    | "Call API" >> beam.ParDo(fetch_data())
   # | "Get first element" >> beam.FlatMap(get_first_element)
)




write_to_bq = (
     cleaned_data
    | "Clean Data to Json" >> beam.Map(data_type_conversion)
    #| beam.io.WriteToText("output")
    | beam.io.WriteToBigQuery('chicago-crime3:data_lake.crime_data',schema=table_schema,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              custom_gcs_temp_location='gs://chicago-crime3-batch/tmp')
)

p1.run()