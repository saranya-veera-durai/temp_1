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

def flatten_json(data, prev_heading='', parent_index='', extracted_tables=None):
    """Recursive function to flatten JSON and extract separate DataFrames for nested arrays."""
    if extracted_tables is None:
        extracted_tables = {}

    if isinstance(data, dict):
        rows = [{}]
        for key, value in data.items():
            full_key = prev_heading + '.' + key if prev_heading else key
            rows = cross_join(rows, flatten_json(value, full_key, parent_index, extracted_tables))
    elif isinstance(data, list):
        rows = []
        array_name = prev_heading.split('.')[-1]  # Get the name of the current array dynamically
        for index, item in enumerate(data):
            # Create the full index key that includes parent array name, index, and child array name
            current_index = f'{parent_index}_{array_name}_{index}' if parent_index else f'{array_name}_{index}'
            flattened_item = flatten_json(item, prev_heading, current_index, extracted_tables)
            for elem in flattened_item:
                # Store the full index including both array names and indexes
                elem[f'{prev_heading.replace(".", "_")}_index'] = current_index
                rows.append(elem)
        
        # Store the rows for this array in a separate DataFrame/table
        if rows:
            table_name = prev_heading.replace(".", "_")
            if table_name not in extracted_tables:
                extracted_tables[table_name] = []
            extracted_tables[table_name].extend(rows)
    else:
        rows = [{prev_heading.replace(".", "_"): data}]
    
    return rows

def extract_json_tables(data_in):
    """Main function to extract tables (DataFrames) from nested JSON."""
    extracted_tables = {}
    flatten_json(data_in, extracted_tables=extracted_tables)
    
    # Convert extracted tables (list of dictionaries) to DataFrames
    dataframes = {key: pd.DataFrame(value) for key, value in extracted_tables.items()}
    
    return dataframes

# File paths
file = "insuranceData_20240909000000.json"

# Load the JSON file and process it
with open(file, encoding="utf8") as f:
    data = json.load(f)  # Load the entire JSON

# Extract tables (DataFrames) from the JSON
extracted_dfs = extract_json_tables(data)

# Save each table (DataFrame) to a separate CSV file
for table_name, df in extracted_dfs.items():
    output_file = f"{table_name}.csv"
    df.to_csv(output_file, mode='w', index=False)
    print(f"CSV output saved to {output_file}")
