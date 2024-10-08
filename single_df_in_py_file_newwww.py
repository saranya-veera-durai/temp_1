
################################################################### SCHEMA_INFO ##############################################################################################
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, ArrayType
# import pandas as pd
# import time
# # Create a Spark session
# spark = SparkSession.builder \
#     .appName("JSON Schema Extraction and Categorization") \
#     .getOrCreate()

# # Path to the JSON file
# json_path = "dmDetails_20241004000000.json"

# # Read the JSON file into a DataFrame
# df = spark.read.json(json_path)

# # Recursive function to flatten the schema
# def flatten_schema(schema, prefix=None):
#     fields = []
#     for field in schema.fields:
#         field_name = field.name if prefix is None else f"{prefix}.{field.name}"
#         field_type = "struct" if isinstance(field.dataType, StructType) else \
#                      f"array<struct>" if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType) else \
#                      f"array<{field.dataType.elementType.simpleString()}>" if isinstance(field.dataType, ArrayType) else \
#                      field.dataType.simpleString()
#         fields.append((field_name, field_type))
#         if isinstance(field.dataType, StructType):
#             fields.extend(flatten_schema(field.dataType, field_name))
#         elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
#             fields.extend(flatten_schema(field.dataType.elementType, field_name))
#     return fields

# # Extract and flatten the schema
# flattened_schema = flatten_schema(df.schema)

# # Convert to DataFrame
# schema_df = pd.DataFrame(flattened_schema, columns=["Field", "Data Type"])

# # Determine hierarchy
# def find_hierarchy(column):
#     return '.'.join(column.split('.')[:-1]) if '.' in column else 'root'

# schema_df["Hierarchy"] = schema_df["Field"].apply(find_hierarchy)

# # Reorder columns
# schema_df = schema_df[["Field", "Hierarchy", "Data Type"]]

# # Write the DataFrame to a CSV file
# schema_df.to_csv("schema_info.csv", index=False)

# time.sleep(1)
# print("CSV file 'schema_info.csv' has been created successfully.")

#################################################################### DATAFRAMES IN SEPERATE PY FILES ######################################################################################################################

import os
import json
import pandas as pd

# Load JSON data from a separate file
def load_json_file(file_path):
    with open(file_path, 'r',encoding='utf-8') as f:
        return [json.loads(line) for line in f]

json_file_path_2 = "dmDetails_20241004000000.json"
# Common PySpark code to be written to each generated file
common_code = f"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode, regexp_replace, date_format
import pandas as pd

spark = SparkSession.builder \\
    .appName("Expand Struct Fields") \\
    .config("spark.executor.memory", "16g") \\
    .config("spark.driver.memory", "16g") \\
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.caseSensitive", True)
json_path ="{json_file_path_2}"
df = spark.read.json(json_path)

# Function to drop child arrays
# Function to drop child arrays
def drop_child_arrays(df):
    def find_array_columns(schema, prefix=''):
        array_columns = []
        for field in schema.fields:
            field_name = f"{{prefix}}.{{field.name}}" if prefix else field.name
            if isinstance(field.dataType, ArrayType):
                array_columns.append(field_name)
            elif isinstance(field.dataType, StructType):
                array_columns += find_array_columns(field.dataType, field_name)
        return array_columns
    
    array_columns = find_array_columns(df.schema)
    df_no_arrays = df.drop(*array_columns)
    
    return df_no_arrays
    
# Recursive function to expand nested structs until there are no more struct columns
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
# Recursive function to expand nested structs until there are no more struct columns
def expand_structs(df: DataFrame) -> DataFrame:
    while True:
        print([field.name for field in df.schema.fields])
        struct_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
        if not struct_columns:
            break
        for struct_col in struct_columns:
            struct_fields = df.schema[struct_col].dataType.fieldNames()
            expanded_fields = [f"{{struct_col}}.{{field}}" for field in struct_fields]
            aliases = [f"{{struct_col}}_{{field}}" for field in struct_fields]  # Add suffix to avoid ambiguity
            df = df.select("*", *[df[expanded_field].alias(alias) for expanded_field, alias in zip(expanded_fields, aliases)])
            df = df.drop(struct_col)
    return df   
"""


# Function to filter rows based on the input hierarchy (for generating PySpark code)
def filter_rows_by_exact_hierarchy(df, hierarchy):
    hierarchy_levels = hierarchy.split('.')
    filtered_rows = []

    # Loop through each row in the DataFrame and check if it belongs to the hierarchy
    for _, row in df.iterrows():
        field = row['Field']
        field_levels = field.split('.')

        # If the field is part of the hierarchy and matches the hierarchy levels, add it
        if len(field_levels) <= len(hierarchy_levels):
            if field_levels == hierarchy_levels[:len(field_levels)]:
                filtered_rows.append(row)

    # Convert the filtered rows back to a DataFrame
    filtered_df = pd.DataFrame(filtered_rows)
    return filtered_df

# Function to generate PySpark code for each hierarchy
def generate_pyspark_code(fields, df_name):
    current_alias = fields.iloc[0]['Field'].split('.')[0]  # Start from the base field
    level_stack = [current_alias]  # To keep track of hierarchy levels

    # PySpark code block as string
    code_str = f'df_{df_name} = df\n'

    for _, field in fields.iterrows():
        field_name = field["Field"]
        data_type = field["Data Type"]
        levels = field_name.split('.')  # Get hierarchy levels
        last_level = levels[-1]  # Get the last part of the field name

        # Handle the hierarchy
        while len(level_stack) > 1 and level_stack[-1] != levels[-2]:
            level_stack.pop()

        if data_type.startswith("array<struct>"):
            alias_name = f"new_{last_level}"
            code_str += f'df_{df_name} = df_{df_name}.select(explode("{last_level}").alias("{alias_name}"))\n'
            code_str += f'df_{df_name} = df_{df_name}.select("{alias_name}.*")\n'
            current_alias = alias_name
            level_stack.append(last_level)

        elif data_type.startswith("struct"):
            if len(levels) > 1 and level_stack:
                code_str += f'df_{df_name} = df_{df_name}.select("{last_level}.*")\n'
                current_alias = f"{current_alias}.{last_level}"
            else:
                code_str += f'df_{df_name} = df_{df_name}.select("{last_level}.*")\n'
                current_alias = last_level
            level_stack.append(last_level)


    # Call expand_structs to dynamically expand any struct fields present
    code_str += f'df_{df_name} = expand_structs(df_{df_name})\n'
    # Drop the exploded columns and aliases in a single line
    code_str += f'df_{df_name} = drop_child_arrays(df_{df_name})\n'
    
    code_str += f'df_{df_name}.printSchema()\n'
    # code_str += f'df_{df_name}.show()\n'
    code_str += f'df_{df_name}.toPandas().to_csv("{df_name}.csv")'
    return code_str

# Function to create Python files for each array of structs found
def create_python_files(arrays, schema_file_path):
    df_csv = pd.read_csv(schema_file_path)  # Read schema CSV into a DataFrame

    for hierarchy in arrays:
        # Replace special characters to create valid file names
        file_name = hierarchy.replace('.', '_').replace('[', '_').replace(']', '') + ".py"
        # Create a directory if necessary
        dir_path = os.path.dirname(file_name)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # Filter schema for the current hierarchy
        filtered_df = filter_rows_by_exact_hierarchy(df_csv, hierarchy)
        
        if not filtered_df.empty:
            df_name = hierarchy.replace('.', '_')

            # Generate PySpark code for this hierarchy
            pyspark_code = generate_pyspark_code(filtered_df, df_name)

            # Combine common code with generated PySpark code
            full_code = f"{common_code}\n\n{pyspark_code}"

            # Create and write to the Python file
            with open(file_name, 'w') as f:
                f.write(f"# PySpark DataFrame code for hierarchy: {hierarchy}\n")
                f.write(full_code)
            # print(f"Created file: {file_name}")
        else:
            print(f"No matching schema found for hierarchy: {hierarchy}")

# Main function to process the JSON and generate Python files for arrays of structs
def process_json(schema_file_path):
    schemaa=pd.read_csv(schema_file_path)
    print(schemaa)
    struct_fields = schemaa[schemaa["Data Type"] == "array<struct>"]["Field"].to_list()
        
    print(struct_fields)
    create_python_files(struct_fields, schema_file_path)

# Example usage: specify the paths to the JSON and schema CSV files
schema_file_path = "schema_info.csv"
process_json( schema_file_path)


#####################################################################################################################################################################################################################










