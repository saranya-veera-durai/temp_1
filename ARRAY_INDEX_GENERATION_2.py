
from copy import deepcopy
import pandas as pd
import json

def cross_join(left, right):
    """Helper function to perform a cross join on dictionaries."""
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows

def flatten_json(data, prev_heading='', parent_index=''):
    """Recursive function to flatten JSON and handle lists with parent array names and index."""
    if isinstance(data, dict):
        rows = [{}]
        for key, value in data.items():
            full_key = prev_heading + '.' + key if prev_heading else key
            rows = cross_join(rows, flatten_json(value, full_key, parent_index))
    elif isinstance(data, list):
        rows = []
        array_name = prev_heading.split('.')[-1]  # Get the name of the current array dynamically
        for index, item in enumerate(data):
            # Create the full index key that includes parent array name, index, and child array name
            current_index = f'{parent_index}_{array_name}_{index}' if parent_index else f'{array_name}_{index}'
            flattened_item = flatten_json(item, prev_heading, current_index)
            for elem in flattened_item:
                # Store the full index including both array names and indexes
                elem[f'{prev_heading.replace(".", "_")}_index'] = current_index
                rows.append(elem)
    else:
        rows = [{prev_heading.replace(".", "_"): data}]
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

