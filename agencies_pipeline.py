import requests
import json 
import configparser
from lib.data_field_mapping import map_data_json
import csv
import os

mapping_path = './Json_mappings/agencies_mapping.json'
export_path = './.temp/'
export_file ='agencies.csv'

try:
    os.mkdir(export_path)
except FileExistsError as e:
    print('Non-critical exception: ', e)

parser = configparser.ConfigParser()
parser.read('./pipeline.conf')
url = parser.get('API_credentials','agency_url')
api_key = parser.get('API_credentials','X-RapidAPI-Key')
api_host = parser.get('API_credentials','X-RapidAPI-Host')

querystring = {"geo_area":"35.80176,-78.64347|35.78061,-78.68218","callback":"call"}

headers = {
	"X-RapidAPI-Key": api_key,
	"X-RapidAPI-Host": api_host
}

#get requests data. 
response = requests.request("GET", url, headers=headers, params=querystring)
responce_json = json.loads(response.text)

#create file meta data.
meta_data = {}
meta_data['api_version'] = responce_json['api_version']
meta_data['generated_on'] = responce_json['generated_on']

#create flat data.
with open(mapping_path,'r') as fh:
    data_mapping = json.loads(fh.read())

header, data = map_data_json(data = responce_json['data'], data_map= data_mapping, additional_attributes = {'callback':'call'})

with open(export_path + export_file,'w') as fh:
    csvw = csv.writer(fh, delimiter=',')
    csvw.writerow(header)
    csvw.writerows(data)



#send data up to azure blob storage.
