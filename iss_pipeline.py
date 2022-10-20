import os
from azure.storage.blob import BlobServiceClient
import configparser
import requests 
import json
from jsonschema import validate, exceptions
import csv
from datetime import datetime
import shutil



#setup, load file paths and create output file name.
schema_path ="./json_schemas/iss_now_schema.json" #schema validation file path.
local_path ='./.temp/'
timestamp_str = str(datetime.now().timestamp()).split(".")[0]
local_file_name = f'iss_now_{timestamp_str}.csv'

#Try to create temp folder, raise soft exeption if already exists.
try:
    os.mkdir(local_path)
except FileExistsError as e:
    print('Non-critical exception: ', e)


#Get request to the API. 
responce = requests.get("http://api.open-notify.org/iss-now.json")
responce_json = json.loads(responce.content)

#Parse JSON schema and validate the API's responce.
with open(schema_path, 'r') as fh:
    schema = fh.read()
    schema = json.loads(schema)

#Will raise an exception if the responce is not valid. 
try:
    validate(instance = responce_json, schema = schema )
    print('\nJSON schema validation passed.')
except exceptions.ValidationError as e:
    raise e
#flatten the reponce.
result = [    
    responce_json["iss_position"]["latitude"],
    responce_json["iss_position"]["longitude"],
    responce_json["timestamp"]
]

#Create headers.
keys = [
    'latitude',
    'longitude',
    'timestamp'
    ]

#Create csv file.
with open(local_path + local_file_name,'w') as fh:
    csvw = csv.writer(fh, delimiter = ',')
    csvw.writerows([keys, result])

#Once the data has been validated and written to a csv retrive the azure blob storage account credentials.
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
connection_string = parser.get('blob_credentials','connection_string')
container_name = parser.get('blob_credentials','container_name')

#create instance of BlobServiceClient using connection string.
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container = container_name, blob = local_file_name)

# Upload the created file.
with open(local_path + local_file_name, "rb") as data:
    print("\nUploading file to Azure Storage:\n\t" + local_file_name)
    
    # provide extra meta data for the uploaded file. Will help with the next steps in the ELT process!
    blob_client.upload_blob(data = data, metadata = {"timestamp":"number",
                                                    "latitude":"string", 
                                                    "longitude":"string"})
print('\nFile sucessfully uploaded.')

#clean up temp files onces loaded to blob.
shutil.rmtree(os.path.dirname(local_path))

