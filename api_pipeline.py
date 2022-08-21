from genericpath import exists
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
timestamp_str = str(datetime.now().timestamp()).split(".")[0]
local_path = "./data" 
schema_path ="./json_schemas/iss_now_schema.json"
export_file_path ='./.temp'
export_file_name = f'iss_now_{timestamp_str}.csv'

try:
    os.mkdir(local_path)
except FileExistsError as e:
    print('Non-critical exception: ', e)

try:
    os.mkdir(export_file_path)
except FileExistsError as e:
    print('Non-critical exception: ', e)
    #shutil.rmtree(os.path.dirname(export_file_path))


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
with open(export_file_path + "/" + export_file_name,'w') as fh:
    csvw = csv.writer(fh, delimiter = '|')
    csvw.writerow(result)

#load credentials for azure blob.
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
connection_string = parser.get('blob_credentials','connection_string')
container_name = parser.get('blob_credentials','connection_string')

#Creating the BlobServiceClient with account url and credential.
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container = container_name, blob = export_file_name)

print("\nUploading to Azure Storage as blob:\n\t" + export_file_name)

# Upload the created file
with open(export_file_path + "/" + export_file_name, "rb") as data:
    blob_client.upload_blob(data)


#clean up temp files onces loaded to blob.
#shutil.rmtree(os.path.dirname(export_file_path))

