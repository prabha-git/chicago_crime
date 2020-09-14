import base64
import os

import google.cloud.logging
from flask import Flask, request
import logging

import sodapy_helper_functions as spy
import gbq_helper_functions as gbq

import json
import numpy as np
from datetime import datetime,timedelta


app = Flask(__name__)


@app.route('/',methods=['POST'])
def data_refresh():
    
    log_client = google.cloud.logging.Client()
    log_client.setup_logging()
    
    logging.info('Refresh Started')
    
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400
    
    
    # Extract the message from  subscription message.
    pubsub_message = envelope['message']
    
    
    
    if isinstance(pubsub_message,dict) and 'data' in pubsub_message:
        message = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()
        logging.info(f'message from pubsub {message}')
        
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']='./chicago-crime-key.json'
    
    
    # get the updated data in last days.
    updated_last_n_days = 2
    logging.info(f'Start Pulling the chicago crime updates in the last {updated_last_n_days} days')
    crimes_df = spy.get_chicago_crime_data(updated_last_n_days=updated_last_n_days)
    logging.info(f'Pulled {crimes_df.shape[0]} records from the chicago portal')
    
    
    
    logging.info('Starting the datatype conversion')
    # Data type conversion
    date_columns = ['updated_on','date']
    for col in date_columns:
        crimes_df[col] = crimes_df[col].astype('datetime64[ns]')
    
    float_columns = ['x_coordinate','y_coordinate','latitude','longitude']
    for col in float_columns:
        crimes_df[col] = crimes_df[col].astype('float')
    

    int_columns = ['id','year']
    for col in int_columns:
        crimes_df[col] = crimes_df[col].astype('int64')
    
    bool_columns = ['arrest','domestic']
    for col in bool_columns:
        crimes_df[col] = crimes_df[col].astype('bool')
    
    
        
    crimes_df['location'] = crimes_df['location'].apply(lambda x: json.dumps(x))
    
    crimes_df = crimes_df.replace({np.nan: None})
    
    logging.info('Ending the datatype conversion')
        
    
    logging.info('Staring Pulling all the id and latest updated timestamp from GQB')
    # Get the records and it's updated date from GBQ
    gbq_crimes = gbq.get_id_arrest_updatedon()
    logging.info(f'Pulled {gbq_crimes.shape[0]} records from GQB')
    
    
    columns = ['id','case_number','date','block','iucr','primary_type','description','location_description','arrest','domestic','beat','district','ward','community_area','fbi_code','x_coordinate',
           'y_coordinate','year','updated_on','latitude','longitude','location']
    
    new_rows=[]
    inactive_rows=[]
    
    logging.info('Starting Looping through each record from chicago portal and see if that needs to updated in GBQ')
    for idx,row in crimes_df.iterrows():
        c=()
        if (len(gbq_crimes[gbq_crimes['id']==row['id']]['id'].values)) >= 1 and (row['updated_on'] > gbq_crimes[gbq_crimes['id']==row['id']]['updated_on']).values[0] and (gbq_crimes['is_active']==True).values[0]:

            inactive_rows.append(row['id'])
            for col in columns:
                c=c+(row[col],)
            # arrest_date
            if row['arrest'] and not (gbq_crimes[gbq_crimes['id']==row['id']]['arrest']).values[0]:
                c=c+(row['updated_on'].date(),)
            else:
                c=c+(None,)
            # is_active flag
            c=c+(True,)
            new_rows.append(c)
        elif  len(gbq_crimes[gbq_crimes['id']==row['id']]['id'].values)==0:
            for col in columns:
                c=c+(row[col],)
            #Arrest Date
            if row['arrest']:
                c=c+(row['updated_on'].date(),)
            else:
                c=c+(None,)
            c=c+(True,) 
            new_rows.append(c)
        
    # Update the records in GBQ to be inactice
    if len(inactive_rows) > 0:
        gbq.set_rows_inactive(inactive_rows)
    else:
        logging.info('All the records are upto date in GBQ!')
        


    # Insert in the batches of 5,000 rows
    for i in range(0,len(new_rows),5000):
        print(gbq.insert_to_bigquery(new_rows[i:i+5000],'crime_dataset','chicago_crime1'))
        logging.info(f'inserting {i}th/st batch of 5000 rows')

    logging.info('refresh mocule complete - exiting!')
    return ('Success', 204)


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080
    
    app.run(host='127.0.0.1',port=PORT,debug=True)