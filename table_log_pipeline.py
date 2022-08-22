from azure.data.tables import TableServiceClient
import configparser

#get storage account credentials. 
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
connection_string = parser.get('blob_credentials', 'connection_string')
table_url = parser.get('blob_credentials', 'table_url')

#establish service instance 
service = TableServiceClient(endpoint= table_url, connection_string=connection_string)