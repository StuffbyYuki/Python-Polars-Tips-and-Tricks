import polars as pl
from google.cloud import bigquery

client = bigquery.Client()

QUERY = (
    '''
        SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` 
        WHERE state = 'TX'
        LIMIT 100
    '''
    )

query_job = client.query(QUERY)  # API request
rows = query_job.result()        # Waits for query to finish

df = pl.from_arrow(rows.to_arrow())
print(df.head())