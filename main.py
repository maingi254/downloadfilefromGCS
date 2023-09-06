#import the libraries
import pandas as pd
from google.cloud import storage
import io
import polars as pl

#connect with google cloud using key
client = storage.Client.from_service_account_json('.\etlprocess-398014-5e9d9f6e9aa2.json')
bucket = client.get_bucket('kelvindatastore')
print(bucket)

#read data 
blob = bucket.get_blob('debit_card_transactions.csv')

#download the data
csv_data = blob.download_as_string()
csv_string = csv_data.decode('utf-8')
#read the data toa dataframe
df=pd.read_csv(io.StringIO(csv_string))
print(df.head())
#do some transformations
df.groupby(['cust_id','transaction_category'])
print(df.head())
#upload csv to google cloud storage
bucket_name='kelvindatastore'
blob_name='datatrans.csv'


storage_client=storage.Client.from_service_account_json('.\etlprocess-398014-5e9d9f6e9aa2.json')
bucket=storage_client.bucket(bucket_name)
blob=bucket.blob(blob_name)

blob.upload_from_string(df.to_csv(),'text/csv')