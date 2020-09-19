from google.cloud import bigquery

# /Users/prabha/GitHub/chicago_crime/chicago-crime-key.json

def get_id_arrest_updatedon():
    """
    Gets the id,arrest,updated_on & is_active info for all the past crime records from GBQ
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
    Inserts the rows into GBQ
    
    Args:
        rows_to_insert(list of tuples): Rows to insert
        dataset(string): GBQ dataset which has the table that we want to insert
        table(string): table to insert the records
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
    Marks the rows as inactive, by setting the column is_active to False
    
    Args:
        inactive_rows(list of integers): case ids which needs to marked as inactive
    """
    inactive_rows_string = ','.join(map(str,inactive_rows))
    bq_client = bigquery.Client()
    query = "update `chicago-crime-284514.crime_dataset.chicago_crime1` set is_active = False where id in ("+inactive_rows_string+")"
    print("Starting the GBQ job to update the flag to inactive")
    query_job = bq_client.query(query).result()
    print("GBQ job to update the flag to inactive is complete")
    
