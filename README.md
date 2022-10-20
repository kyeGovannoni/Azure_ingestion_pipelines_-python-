# pipelines

The purpose of this project is to test out some of the concepts discussed in the book 'data pipelines pocket reference'. Specifically creating a data pipeline in python that ingests data from an external api and outputs a flat file.

To make the project more interesting I decided that the output file should be stored in and Azure blob storage container. Further concepts/additions.

iss_pipeline impliments a JSON schema file to validate the API data before processing and outputing it.

agencies_pipeline has a data mapping function for defining data elements and de-nesting json objects. 

The json validation schema file can be found at ./json_schemas/iss_now_schema.json 

The file used to map data elements for unpacking or selectively picking nested json objects can be found at ./json_mappings/agencies_mapping.json

The folder lib contains the function used to process the json mapping file.

A file called pipeline_conf_template.conf is included to show what the config file used looks like minus the actual credentials. 
For refrence this excluded file is named pipeline.conf
