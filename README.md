# pipelines

There are two pipline files iss_piplines.py and agencies_pipline.py. 

The json validation schema file can be found at ./json_schemas/iss_now_schema.json 

The file used to map data elements for unpacking or selectively picking nested json objects can be found at ./json_mappings/agencies_mapping.json

The folder lib contains the function used to process the json mapping file.

A file called pipeline_conf_template.conf is included to show what the config file used looks like minus the actual credentials. 
For refrence this excluded file is named pipeline.conf
