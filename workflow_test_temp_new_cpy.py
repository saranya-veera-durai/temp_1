############################################ 5 DATAFRAMES IN ONE PY FILE #############################################################################

# import os
# import json
# import pandas as pd
# import boto3

# # Load JSON data from a separate file
# def load_json_file(file_path):
#     with open(file_path, 'r', encoding='utf-8') as f:
#         return [json.loads(line) for line in f]

# file_parent_name = "insuranceData"

# json_file_path_2 = f"s3://datawarehouse-bilight/LentraData/LOS3.0/{file_parent_name}/Daily/2024/10/08/insuranceData_20241008000000.json"

# # Common PySpark code to be written to each generated file
# common_code = f"""
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, ArrayType
# from pyspark.sql.functions import col, posexplode, regexp_replace, date_format
# import pandas as pd

# spark = SparkSession.builder \\
#     .appName("Expand Struct Fields") \\
#     .config("spark.executor.memory", "16g") \\
#     .config("spark.driver.memory", "16g") \\
#     .getOrCreate()

# spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
# spark.conf.set("spark.sql.caseSensitive", True)
# json_path ="{json_file_path_2}"
# df = spark.read.json(json_path)

# # Function to drop child arrays
# def drop_child_arrays(df):
#     def find_array_columns(schema, prefix=''):
#         array_columns = []
#         for field in schema.fields:
#             field_name = f"{{prefix}}.{{field.name}}" if prefix else field.name
#             if isinstance(field.dataType, ArrayType):
#                 array_columns.append(field_name)
#             elif isinstance(field.dataType, StructType):
#                 array_columns += find_array_columns(field.dataType, field_name)
#         return array_columns
    
#     array_columns = find_array_columns(df.schema)
#     df_no_arrays = df.drop(*array_columns)
    
#     return df_no_arrays

# # Recursive function to expand nested structs until there are no more struct columns
# from pyspark.sql import DataFrame
# from pyspark.sql.types import StructType

# def expand_structs(df: DataFrame) -> DataFrame:
#     while True:
#         struct_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
#         if not struct_columns:
#             break
#         for struct_col in struct_columns:
#             struct_fields = df.schema[struct_col].dataType.fieldNames()
#             expanded_fields = [f"{{struct_col}}.{{field}}" for field in struct_fields]
#             aliases = [f"{{struct_col}}_{{field}}" for field in struct_fields]  # Add suffix to avoid ambiguity
#             df = df.select("*", *[df[expanded_field].alias(alias) for expanded_field, alias in zip(expanded_fields, aliases)])
#             df = df.drop(struct_col)
#     return df   
# """

# # Function to filter rows based on the input hierarchy (for generating PySpark code)
# def filter_rows_by_exact_hierarchy(df, hierarchy):
#     hierarchy_levels = hierarchy.split('.')
#     filtered_rows = []

#     # Loop through each row in the DataFrame and check if it belongs to the hierarchy
#     for _, row in df.iterrows():
#         field = row['Field']
#         field_levels = field.split('.')

#         # If the field is part of the hierarchy and matches the hierarchy levels, add it
#         if len(field_levels) <= len(hierarchy_levels):
#             if field_levels == hierarchy_levels[:len(field_levels)]:
#                 filtered_rows.append(row)

#     # Convert the filtered rows back to a DataFrame
#     filtered_df = pd.DataFrame(filtered_rows)
#     return filtered_df

# def generate_pyspark_code(fields, df_name, s3_bucket, s3_prefix):
#     current_alias = fields.iloc[0]['Field'].split('.')[0]  # Start from the base field
#     level_stack = [current_alias]  # To keep track of hierarchy levels

#     # PySpark code block as string
#     code_str = f"df_{df_name} = df.select(col('_id').alias('root_id'), '*')\n"
#     print(f"Processing DataFrame: {df_name}")

#     pos_fields = ["root_id"]

#     for _, field in fields.iterrows():
#         field_name = field["Field"]
#         data_type = field["Data Type"]
#         levels = field_name.split('.')  # Get hierarchy levels
#         last_level = levels[-1] if levels else None
        
#         # Handle the hierarchy
#         if len(levels) >= 2:
#             while len(level_stack) > 1 and level_stack[-1] != levels[-2]:
#                 level_stack.pop()

#         if data_type.startswith("array<struct>"):
#             alias_name = f"new_{last_level}"
#             code_str += f"df_{df_name} = df_{df_name}.select(*{pos_fields}, posexplode('{last_level}').alias('{last_level}_pos', '{alias_name}'))\n"
#             code_str += f"df_{df_name} = df_{df_name}.select(*{pos_fields}, '{last_level}_pos', '{alias_name}.*')\n"
#             pos_fields.append(f"{last_level}_pos")
#             current_alias = alias_name
#             level_stack.append(last_level)

#         elif data_type.startswith("struct"):
#             code_str += f"df_{df_name} = df_{df_name}.select(*{pos_fields}, '{last_level}.*')\n"
#             current_alias = last_level
#             level_stack.append(last_level)

#     # Call expand_structs to dynamically expand any struct fields present
#     code_str += f"df_{df_name} = expand_structs(df_{df_name})\n"
#     # Drop the exploded columns and aliases in a single line
#     code_str += f"df_{df_name} = drop_child_arrays(df_{df_name})\n"
#     code_str += f"df_{df_name}.printSchema()\n"
    
#     s3_output_path = f"s3://{s3_bucket}/{s3_prefix}/{df_name}.csv"
#     code_str += f"df_{df_name}.toPandas().to_csv('{s3_output_path}', index=False)\n"
    
#     return code_str

# def create_python_files(arrays, schema_file_path, s3_bucket, s3_prefix):
#     s3 = boto3.client('s3')
#     df_csv = pd.read_csv(schema_file_path)
#     file_counter = 1

#     for i in range(0, len(arrays), 5):
#         array_group = arrays[i:i + 5]
#         file_name = f"insurance_arrays_{file_counter}.py"
#         s3_key = f"{s3_prefix}/{file_name}"

#         # PySpark code to hold for the current file
#         pyspark_code = ""

#         for hierarchy in array_group:
#             filtered_df = filter_rows_by_exact_hierarchy(df_csv, hierarchy)
#             if not filtered_df.empty:
#                 # Replace dots with underscores for DataFrame names
#                 df_name = hierarchy.replace('.', '_')
#                 # Generate PySpark code for the current array
#                 pyspark_code += generate_pyspark_code(filtered_df, df_name, s3_bucket, s3_prefix)
#                 file_counter += 1

#         if pyspark_code:
#             # Combine common code with generated PySpark code
#             full_code = f"{common_code}\n\n{pyspark_code}"

#             # Upload the Python code to S3
#             s3.put_object(Body=full_code, Bucket=s3_bucket, Key=s3_key)
#             print(f"Uploaded file to S3: s3://{s3_bucket}/{s3_key}")

# # Main function to process the JSON and generate Python files for arrays of structs
# def process_json(schema_file_path, s3_bucket, s3_prefix):
#     schemaa = pd.read_csv(schema_file_path)
#     struct_fields = schemaa[schemaa["Data Type"] == "array<struct>"]["Field"].to_list()
#     create_python_files(struct_fields, schema_file_path, s3_bucket, s3_prefix)

# # Example usage: specify the paths to the JSON and schema CSV files
# schema_file_path = "s3://datawarehouse-bilight/Staging/LOS3.0/dmDetails/Yearly/testing/08_schema_info.csv"
# s3_bucket = 'datawarehouse-bilight'
# s3_prefix = 'Staging/LOS3.0/dmDetails/Yearly/test'

# process_json(schema_file_path, s3_bucket, s3_prefix)

################################################## ADD EXTRA TRIGGER WITH ONE JOB  ######################################################################
import boto3

# Initialize the Glue client
glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

def get_job_names(prefix):
    job_names = []
    next_token = None
    while True:
        if next_token:
            response = glue_client.list_jobs(NextToken=next_token)
        else:
            response = glue_client.list_jobs()
        job_names.extend(response['JobNames'])
        next_token = response.get('NextToken')
        if not next_token:
            break

    # Filter job names based on the provided prefix
    filtered_job_names = [job for job in job_names if job.startswith(prefix)]
    return filtered_job_names

# Automate retrieval of job names for the specified prefixes
insurance_jobs = get_job_names("LOS3.0_insuranceData__")
insuranceData = list(insurance_jobs)

# Divide jobs into sublists of size 3
chunk_size_nested = 3
sublists = [insuranceData[i:i + chunk_size_nested] for i in range(0, len(insuranceData), chunk_size_nested)]

# Function to create workflows dynamically
def create_workflow(workflow_name, description=''):
    try:
        response = glue_client.create_workflow(
            Name=workflow_name,
            Description=description
        )
        print(f"Workflow '{workflow_name}' created successfully.")
        return response
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Workflow '{workflow_name}' already exists.")
    except Exception as e:
        print(f"Error creating workflow: {e}")


def create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=None):
    try:
        # Check if the trigger already exists
        try:
            existing_trigger_response = glue_client.get_trigger(Name=trigger_name)
            existing_trigger = existing_trigger_response['Trigger']
            print(f"Trigger '{trigger_name}' already exists, updating it.")
            
            new_actions = [{'JobName': job} for job in job_names]
            predicate = None
            if previous_job_name:
                predicate = {
                    'Logical': 'AND',
                    'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': previous_job_name, 'State': 'SUCCEEDED'}]
                }
            trigger_response = glue_client.update_trigger(
                Name=trigger_name,
                TriggerUpdate={
                    'Name': trigger_name,
                    'Actions': new_actions,
                    'Predicate': predicate
                }
            )
            print(f"Trigger '{trigger_name}' updated successfully.")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"Trigger '{trigger_name}' not found, creating it.")
            if previous_job_name:
                trigger_response = glue_client.create_trigger(
                    Name=trigger_name,
                    Type='CONDITIONAL',
                    Actions=[{'JobName': job} for job in job_names],
                    WorkflowName=workflow_name,
                    Predicate={
                        'Logical': 'AND',
                        'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': previous_job_name, 'State': 'SUCCEEDED'}]
                    },
                    StartOnCreation=False
                )
            else:
                trigger_response = glue_client.create_trigger(
                    Name=trigger_name,
                    Type='ON_DEMAND',
                    Actions=[{'JobName': job} for job in job_names],
                    WorkflowName=workflow_name,
                    StartOnCreation=False
                )
            print(f"Trigger '{trigger_name}' created successfully.")
        return trigger_response
    except Exception as e:
        print(f"Error creating or updating trigger '{trigger_name}': {e}")
        return None

def activate_trigger(trigger_name):
    try:
        response = glue_client.start_trigger(Name=trigger_name)
        print(f"Trigger '{trigger_name}' activated successfully.")
        return response
    except Exception as e:
        print(f"Error activating trigger '{trigger_name}': {e}")
        return None

# Function to create and activate triggers in sequence, including the extra job
def create_and_activate_triggers(sublists, workflow_name):
    previous_job_name = None

    for i, job_names in enumerate(sublists):
        trigger_name = f"{workflow_name}_trigger_{i+1}"  # Dynamic trigger name
        trigger_response = create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=previous_job_name)
        
        if trigger_response:
            activate_trigger(trigger_name)
            previous_job_name = job_names[-1]  # Last job of the current trigger to chain the next trigger
        else:
            print(f"Failed to create the trigger: {trigger_name}")
            break

    # Add an additional trigger with the new job (workflow_test_temp_new_cpy_cpy)
    extra_trigger_name = f"{workflow_name}_extra_trigger"  # New trigger for the extra job
    extra_trigger_response = create_or_update_trigger(extra_trigger_name, ["workflow_test_temp_new_cpy_cpy"], workflow_name, previous_job_name=previous_job_name)
    
    if extra_trigger_response:
        activate_trigger(extra_trigger_name)
    else:
        print(f"Failed to create the extra trigger: {extra_trigger_name}")

# Main function to manage workflows and triggers
def manage_workflows_and_triggers(sublists):
    # Each workflow can handle up to 30 jobs (10 triggers with 3 jobs each)
    max_jobs_per_workflow = 6
    workflow_count = 1

    for i in range(0, len(sublists), 2):  # 10 triggers per workflow, each trigger has 3 jobs
        workflow_sublists = sublists[i:i + 2]  # Get a chunk of 10 triggers (30 jobs)
        
        workflow_name = f"Insurance_Workflow_{workflow_count}"
        create_workflow(workflow_name, f"Workflow for {workflow_name}")
        
        create_and_activate_triggers(workflow_sublists, workflow_name)
        
        workflow_count += 1

# Execute the function to manage workflows and triggers
manage_workflows_and_triggers(sublists)

