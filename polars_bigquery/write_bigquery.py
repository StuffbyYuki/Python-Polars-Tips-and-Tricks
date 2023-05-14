### This is still under development

# from google.cloud import bigquery
# from config import PROJECT_ID

# client = bigquery.Client()

# project_id = PROJECT_ID
# dataset_name = 'sample_datasets'
# dataset_id = f'{project_id}.{dataset_name}'
# destination_table_name = 'sample_public_table'

# # If you want to create a dataset, then you'd need the following 2 lines of code
# # dataset = bigquery.Dataset(dataset_id)
# # dataset = client.create_dataset(dataset)

# job_config = bigquery.QueryJobConfig()
# job_config.destination = f"{dataset_id}.{destination_table_name}"
# # job_config.write_disposition = 'WRITE_TRUNCATE'  # this replaces the existing table

# query = """
#     SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` 
#     WHERE state = 'TX'
#     LIMIT 100
# """

# # Run the query.
# query_job = client.query(query, job_config=job_config)
# # Waits for the query to finish
# query_job.result()
