
def map_data_json(data:list, data_map:dict, additional_attributes:dict = None)-> tuple : 
    ''' takes in a list of data dict object data and returns array of keys and values based on data map'''
    
    if isinstance(data, dict):
        data = [data]

    if additional_attributes:
        add_keys = list(additional_attributes.keys())
    else:
        add_keys = None

    all_passes = []
    keys = list(data_map.keys())
    
    for row in  data:
        current_pass = []
        for key in keys:
            index = data_map[key]
            val = eval(f'row{index}')
            current_pass.append(val)
        
        #append additional parameters.
        if additional_attributes:
            for key in add_keys:
                current_pass.append(additional_attributes[key])
        
        all_passes.append(current_pass)

    if additional_attributes:
        keys += add_keys

    return (keys, all_passes)
        
        
        


