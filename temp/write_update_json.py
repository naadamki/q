import json


## Write

# Your list of dict objects
data = [{"key": "value"}, {"key": "value2"}]

# Write to file
with open('data.json', 'w') as f:
    json.dump(data, f, indent=2)


## Update

import json

# New dict objects to add
new_data = [{"key": "value3"}, {"key": "value4"}]

# Read existing data
with open('data.json', 'r') as f:
    existing_data = json.load(f)

# Append new data
existing_data.extend(new_data)

# Write back to file
with open('data.json', 'w') as f:
    json.dump(existing_data, f, indent=2)



import json

def append_to_json(filename, new_data):
    """Append dict objects to a JSON file"""
    try:
        with open(filename, 'r') as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = []
    
    if isinstance(new_data, dict):
        existing_data.append(new_data)
    else:  # It's a list
        existing_data.extend(new_data)
    
    with open(filename, 'w') as f:
        json.dump(existing_data, f, indent=2)

# Usage:
append_to_json('data.json', [{"key": "value"}])
append_to_json('data.json', {"key": "another"})