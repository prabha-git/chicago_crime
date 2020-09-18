from google.cloud import bigquery

# /Users/prabha/GitHub/chicago_crime/chicago-crime-key.json

def get_id_arrest_updatedon():
    """
    """
    bq_client = bigquery.Client()
    query = """
                SELECT id,
                        arrest,
                        updated_on,
                        is_active
                  FROM `chicago-crime-284514.crime_dataset.chicago_crime1` where is_active=True
            """
    results = bq_client.query(query).result().to_dataframe()
    return(results)


def insert_to_bigquery(rows_to_insert,dataset,table):
    """
    """

           
    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(dataset)
    table_ref=dataset_ref.table(table)
    
    table = bq_client.get_table(table_ref)
    
    errors = bq_client.insert_rows(table,rows_to_insert)
    print(errors)
    
    assert errors == []
    
def set_rows_inactive(inactive_rows):
    """
    """
    inactive_rows_string = ','.join(map(str,inactive_rows))
    bq_client = bigquery.Client()
    query = "update `chicago-crime-284514.crime_dataset.chicago_crime1` set is_active = False where id in ("+inactive_rows_string+")"
    print(query)
    query_job = bq_client.query(query).result()
    print(query_job)
    
