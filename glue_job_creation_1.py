################################################## Needed Input ############################################################

json_file_path = "s3://datawarehouse-bilight/LentraData/LOS3.0/sanctionConditions/Daily/2024/10/01/sanctionConditions_20241001000000.json"  
file_parent_name = "sanctionConditions"

#############################################  Print schema in txt file ###################################################

from pyspark.sql import SparkSession
import s3fs

# Create Spark session
spark = SparkSession.builder \
    .appName("Expand Struct Fields") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "16g") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.caseSensitive", True)

# Load the JSON data
json_path = json_file_path
df = spark.read.json(json_path)

# Capture the schema as a nested tree string
schema_string = df._jdf.schema().treeString()

# Define S3 path where you want to save the schema file
s3_output_path = f"s3://datawarehouse-bilight/Staging/LOS3.0/{file_parent_name}/Yearly/demo/print_schema_output.txt"  # Update with your S3 bucket

# Use s3fs to write the schema to the S3 file
fs = s3fs.S3FileSystem(anon=False)  # Set anon=True if the bucket is public, otherwise configure credentials

with fs.open(s3_output_path, "w") as file:
    file.write(schema_string)

# Confirm output
print(f"Nested schema has been written to {s3_output_path}")



####################### schema_info ########################
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
import pandas as pd
import time
# Create a Spark session
spark = SparkSession.builder \
    .appName("Expand Struct Fields") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "16g") \
    .getOrCreate()

# Path to the JSON file
json_path = json_file_path

# Read the JSON file into a DataFrame
df = spark.read.json(json_path)

# Recursive function to flatten the schema
def flatten_schema(schema, prefix=None):
    fields = []
    for field in schema.fields:
        field_name = field.name if prefix is None else f"{prefix}.{field.name}"
        field_type = "struct" if isinstance(field.dataType, StructType) else \
                     f"array<struct>" if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType) else \
                     f"array<{field.dataType.elementType.simpleString()}>" if isinstance(field.dataType, ArrayType) else \
                     field.dataType.simpleString()
        fields.append((field_name, field_type))
        if isinstance(field.dataType, StructType):
            fields.extend(flatten_schema(field.dataType, field_name))
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            fields.extend(flatten_schema(field.dataType.elementType, field_name))
    return fields

# Extract and flatten the schema
flattened_schema = flatten_schema(df.schema)

# Convert to DataFrame
schema_df = pd.DataFrame(flattened_schema, columns=["Field", "Data Type"])

# Determine hierarchy
def find_hierarchy(column):
    return '.'.join(column.split('.')[:-1]) if '.' in column else 'root'

schema_df["Hierarchy"] = schema_df["Field"].apply(find_hierarchy)

# Reorder columns
schema_df = schema_df[["Field", "Hierarchy", "Data Type"]]

# Write the DataFrame to a CSV file
schema_df.to_csv(f"s3://datawarehouse-bilight/Staging/LOS3.0/{file_parent_name}/Yearly/demo/schema_info.csv", index=False)

time.sleep(1)
print("CSV file 'schema_info.csv' has been created successfully.")



# #################################### DATAFRAMES IN SEPERATE PY FILES ####################################################

import os
import json
import pandas as pd
import boto3


# Load JSON data from a separate file
def load_json_file(file_path):
    with open(file_path, 'r',encoding='utf-8') as f:
        return [json.loads(line) for line in f]


json_path = json_file_path
# Common PySpark code to be written to each generated file
common_code = f"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, posexplode, regexp_replace, date_format ,lit
import pandas as pd

spark = SparkSession.builder \\
    .appName("Expand Struct Fields") \\
    .config("spark.executor.memory", "16g") \\
    .config("spark.driver.memory", "16g") \\
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.caseSensitive", True)
json_path ="{json_path}"
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
            df = df.withColumn("Filename", lit("{json_path.split('/')[-1]}"))
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
def generate_pyspark_code(fields, df_name, s3_bucket, s3_prefix):
    current_alias = fields.iloc[0]['Field'].split('.')[0]  # Start from the base field
    level_stack = [current_alias]  # To keep track of hierarchy levels
      # To store all the _pos fields for use in selections

    # PySpark code block as string
    code_str = f'df_{df_name} = df.select(col("_id").alias("root_id"),"*")\n'
    pos_fields = ["root_id"]
    for _, field in fields.iterrows():
        field_name = field["Field"]
        data_type = field["Data Type"]
        levels = field_name.split('.')  # Get hierarchy levels
        last_level = levels[-1]  # Get the last part of the field name

        # Handle the hierarchy
        while len(level_stack) > 1 and level_stack[-1] != levels[-2]:
            level_stack.pop()

        # Use *pos_fields to unpack the list into separate arguments for the select() method
        if data_type.startswith("array<struct>"):
            alias_name = f"new_{last_level}"
            # Add the pos field to the list
            if pos_fields:
                # Unpack pos_fields when calling select()
                code_str += f'df_{df_name} = df_{df_name}.select(*{pos_fields}, posexplode("{last_level}").alias("{last_level}_pos", "{alias_name}"))\n'
                code_str += f'df_{df_name} = df_{df_name}.select(*{pos_fields}, "{last_level}_pos", "{alias_name}.*")\n'
            else:
                code_str += f'df_{df_name} = df_{df_name}.select(posexplode("{last_level}").alias("{last_level}_pos", "{alias_name}"))\n'
                code_str += f'df_{df_name} = df_{df_name}.select("{last_level}_pos", "{alias_name}.*")\n'
            
            pos_fields.append(f"{last_level}_pos")  # Store pos field
            current_alias = alias_name
            level_stack.append(last_level)

        elif data_type.startswith("struct"):
            # For struct fields, select the current struct with existing _pos fields
            if pos_fields:
                # Unpack pos_fields when calling select()
                code_str += f'df_{df_name} = df_{df_name}.select(*{pos_fields}, "{last_level}.*")\n'
            else:
                code_str += f'df_{df_name} = df_{df_name}.select("{last_level}.*")\n'
            current_alias = last_level
            level_stack.append(last_level)

    # Call expand_structs to dynamically expand any struct fields present
    code_str += f'df_{df_name} = expand_structs(df_{df_name})\n'
    # Drop the exploded columns and aliases in a single line
    code_str += f'df_{df_name} = drop_child_arrays(df_{df_name})\n'

    code_str += f'df_{df_name}.printSchema()\n'
    s3_output_path = f's3://{s3_bucket}/{s3_prefix}/{df_name}.csv'
    code_str += f'df_{df_name}.toPandas().to_csv("{s3_output_path}", index=False)'
    return code_str

# Function to create Python files for each array of structs found

# Function to create Python files for each array of structs found and upload to S3
def create_python_files(arrays, schema_file_path, s3_bucket, s3_prefix):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Read schema CSV into a DataFrame
    df_csv = pd.read_csv(schema_file_path)
    
    for index,hierarchy in enumerate(arrays, start=2):
        # Replace special characters to create valid S3 object names
        file_name = "LOS3.0_"+file_parent_name+"__"+hierarchy.replace('.', '_').replace('[', '_').replace(']', '') +"_"+str(index)+".py"
        
        # S3 key (path) for the file inside the S3 bucket
        s3_key = f"{s3_prefix}/{file_name}"
        
        # Filter schema for the current hierarchy
        filtered_df = filter_rows_by_exact_hierarchy(df_csv, hierarchy)
        
        if not filtered_df.empty:
            df_name = hierarchy.replace('.', '_').replace('-', '')

            # Generate PySpark code for this hierarchy
            pyspark_code = generate_pyspark_code(filtered_df, df_name, s3_bucket, s3_prefix)

            # Combine common code with generated PySpark code
            full_code = f"{common_code}\n\n{pyspark_code}"

            # Upload the Python code to S3
            s3.put_object(Body=full_code, Bucket=s3_bucket, Key=s3_key)
            print(f"Uploaded file to S3: s3://{s3_bucket}/{s3_key}")
        else:
            print(f"No matching schema found for hierarchy: {hierarchy}")

    # Generate the main.py code that includes the common operations
    main_code = f"""
# PySpark DataFrame code for hierarchy: main

main_df = df
main_df = expand_structs(main_df)
main_df = drop_child_arrays(main_df)
main_df.printSchema()
main_df.toPandas().to_csv(f"s3://{s3_bucket}/{s3_prefix}/{file_parent_name}_main.csv", index=False)
"""
    
    # Combine common code with main code
    full_main_code = f"{common_code}\n\n{main_code}"
    
    # Upload main.py to S3
    s3_key_main = f"{s3_prefix}/LOS3.0_"+file_parent_name+"__main_1.py"
    s3.put_object(Body=full_main_code, Bucket=s3_bucket, Key=s3_key_main)
    print(f"Uploaded main.py to S3: s3://{s3_bucket}/{s3_key_main}")

# Main function to process the JSON and generate Python files for arrays of structs
def process_json(schema_file_path, s3_bucket, s3_prefix):
    schemaa=pd.read_csv(schema_file_path)
    print(schemaa)
    struct_fields = schemaa[schemaa["Data Type"] == "array<struct>"]["Field"].to_list()
        
    print(struct_fields)
    create_python_files(struct_fields, schema_file_path, s3_bucket, s3_prefix)

# Example usage: specify the paths to the JSON and schema CSV files
schema_file_path = f"s3://datawarehouse-bilight/Staging/LOS3.0/{file_parent_name}/Yearly/demo/schema_info.csv"
s3_bucket = 'datawarehouse-bilight'  # Replace with your S3 bucket name
s3_prefix = f'Staging/LOS3.0/{file_parent_name}/Yearly/demo'  # Replace with the desired S3 prefix (folder path)

process_json( schema_file_path, s3_bucket, s3_prefix)


####################################### CREATE GLUE JOB ######################################################

import boto3

# Initialize the S3 and Glue clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def list_files_in_s3(bucket_name, prefix):
    """List all files in a given S3 bucket under a specific prefix (folder)."""
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.py')]
    return files

def create_glue_job_for_script(bucket_name, script_key, role_arn, temp_dir):
    """Create a Glue job for each script."""
    job_name = script_key.split('/')[-1].replace('.py', '')  # Job name based on the file name
    
    response = glue_client.create_job(
        Name=job_name,  # Unique job name
        Role=role_arn,  # IAM Role ARN
        Command={
            'Name': 'glueetl',  # 'glueetl' for Spark jobs
            'ScriptLocation': f's3://{bucket_name}/{script_key}',  # Path to the script in S3
            'PythonVersion': '3'  # Use Python 3
        },
        DefaultArguments={
            '--job-language': 'python',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-spark-ui': 'true',  # Enable the Spark UI
            '--enable-glue-datacatalog': 'true',  # Enable Data Catalog integration
            '--TempDir': temp_dir  # Temporary directory for Glue jobs
        },
        MaxRetries=0,  # Number of retries if the job fails
        Timeout=2880,  # Timeout in minutes
        NumberOfWorkers=10,  # Number of workers (for Glue 3.0 or higher)
        WorkerType='G.2X',  # Worker type (G.1X or G.2X)
        GlueVersion='4.0',  # Glue version (2.0, 3.0, or 4.0)
    )
    
    print(f"Job created for script: {script_key} -> Job Name: {response['Name']}")

def create_glue_jobs_for_directory(bucket_name, prefix, role_arn, temp_dir):
    """List files in the directory and create Glue jobs for each."""
    scripts = list_files_in_s3(bucket_name, prefix)
    if not scripts:
        print(f"No scripts found in s3://{bucket_name}/{prefix}")
        return

    # Iterate through the scripts and create a Glue job for each
    for script_key in scripts:
        create_glue_job_for_script(bucket_name, script_key, role_arn, temp_dir)

# Example usage
bucket_name = 'datawarehouse-bilight'
script_prefix = f'Staging/LOS3.0/{file_parent_name}/Yearly/demo/'  # Directory containing the Python scripts in S3
role_arn = 'arn:aws:iam::493316969816:role/Belight-Glue'  # Replace with your IAM role ARN
temp_dir = 's3://aws-glue-assets-493316969816-ap-south-1/temporary/'  # Temporary directory for Glue jobs

# Create Glue jobs for each Python script found in the S3 directory
create_glue_jobs_for_directory(bucket_name, script_prefix, role_arn, temp_dir)
