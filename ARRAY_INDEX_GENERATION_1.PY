
#################################################  INDEX GENERATION ########################################################### 
from copy import deepcopy
import pandas as pd
import json

def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows

def flatten_json(data, prev_heading=''):
    """Recursive function to flatten JSON and handle lists dynamically."""
    if isinstance(data, dict):
        rows = [{}]
        for key, value in data.items():
            rows = cross_join(rows, flatten_json(value, prev_heading + '.' + key))
    elif isinstance(data, list):
        rows = []
        array_name = prev_heading.split('.')[-1]  # Get the name of the array dynamically
        for index, item in enumerate(data):
            index_key = f'{prev_heading.replace(".", "_")}_index'  # Create a unique index key for each nested array
            flattened_item = flatten_json(item, prev_heading)
            for elem in flattened_item:
                elem[index_key] = f'{array_name}_{index}'  # Store the index dynamically
                rows.append(elem)
    else:
        rows = [{prev_heading[1:]: data}]
    return rows

def json_to_dataframe(data_in):
    """Main function to convert JSON to DataFrame."""
    return pd.DataFrame(flatten_json(data_in))

# File paths
file = "insuranceData_20240909000000.json"
output_file = "insuranceData_20240909000000.csv"

# Load the JSON file and process it
with open(file, encoding="utf8") as f:
    data = json.load(f)  # Load the entire JSON

df = json_to_dataframe(data)

# Save the DataFrame to CSV
df.to_csv(output_file, mode='w', index=False)
print(f"CSV output saved to {output_file}")
