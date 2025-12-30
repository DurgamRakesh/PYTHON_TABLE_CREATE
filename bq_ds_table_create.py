# THIS IS A CODE FOR CREATEING A DATASET AND TABLE IN BIGQUERY BY USING PYTHON.
from google.cloud import bigquery

client = bigquery.Client()

print(client)
project_id = 'rd-proj-85'
dataset_id = f'{project_id}.python_ds'
table_id = f'{project_id}.python_ds.python_table'

dataset = bigquery.Dataset(dataset_id)
dataset.location = 'US'
client.create_dataset(dataset,exists_ok=True)

schema = [
    bigquery.SchemaField('cust_id','INT64',mode='REQUIRED', description="details about cust_id columen"),
    bigquery.SchemaField('name','STRING',mode='REQUIRED', description="details of customer name columen"),
    bigquery.SchemaField('salary','INT64',mode='REQUIRED', description="details salary columen")
    
]

table = bigquery.Table(table_id,schema=schema)
client.create_table(table,exists_ok=True)

rows = [
    {"cust_id": 1, "name": "jhon", "salary": 1200},
    {"cust_id": 2, "name": "smith","salary":2000}
]

client.insert_rows_json(table_id,rows)

print('done')