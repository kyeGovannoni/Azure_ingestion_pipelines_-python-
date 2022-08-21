import os
from azure.storage.blob import BlobServiceClient
import configparser
import requests 
import json
from jsonschema import validate
import csv
from datetime import datetime
import shutil


#Make local path for blob files.
schema_path ="./json_schemas/iss_now_schema.json"
local_path ='./.temp/'
timestamp_str = str(datetime.now().timestamp()).split(".")[0]
local_file_name = f'iss_now_{timestamp_str}.csv'

try:
    os.mkdir(local_path)
except FileExistsError as e:
    print('Non-critical exception: ', e)


#Get responce from API. 
responce = requests.get("http://api.open-notify.org/iss-now.json")
responce_json = json.loads(responce.content)

#Get JSON schema and validate responce.
with open(schema_path, 'r') as fh:
    schema = fh.read()
    schema = json.loads(schema)

validate(instance = responce_json, schema = schema )

#flatten the reponce.
result = [    
    responce_json["iss_position"]["latitude"],
    responce_json["iss_position"]["longitude"],
    responce_json["timestamp"]
]

#create csv file.
keys = [
    'latitude',
    'longitude',
    'timestamp'
    ]

with open(local_path + local_file_name,'w') as fh:
    csvw = csv.writer(fh, delimiter = ',')
    csvw.writerows([keys, result])

#get credentials for azure blob.
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
connection_string = parser.get('blob_credentials','connection_string')
container_name = parser.get('blob_credentials','container_name')

#create instance of BlobServiceClient using connection string.
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container = container_name, blob = local_file_name)

# Upload the created file
with open(local_path + local_file_name, "rb") as data:
    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)
    blob_client.upload_blob(data)

#clean up temp files onces loaded to blob.
shutil.rmtree(os.path.dirname(local_path))

