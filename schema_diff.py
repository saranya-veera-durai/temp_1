

import pandas as pd
import s3fs
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import sys

# Initialize Spark and Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# S3 input and output paths
s3_input_path = 's3://datawarehouse-bilight/Staging/LOS3.0/bankVerificationDetailsCustom/Yearly/schemainfofolder/'  
s3_output_path = 's3://datawarehouse-bilight/Staging/LOS3.0/bankVerificationDetailsCustom/Yearly/schemainfofolder/output/fields_with_conflicting_data_types.csv'

# Initialize s3fs
fs = s3fs.S3FileSystem()

# Read all CSV files in the input S3 directory and merge them
merged_df = []

# List all files in the input directory
for file_path in fs.ls(s3_input_path):
    if file_path.endswith('.csv'):
        # Read each CSV file and add a 'filename' column
        df = pd.read_csv(f"s3://{file_path}", storage_options={"anon": False})
        df['filename'] = file_path.split('/')[-1]  # Extract filename
        merged_df.append(df)

# Concatenate all DataFrames
final_df = pd.concat(merged_df, ignore_index=True)

# Select relevant columns for further processing
final_df = final_df[['Field', 'Data Type', 'filename']]

# Group by Field and Data Type, aggregating filenames for each combination
grouped_df = final_df.groupby(['Field', 'Data Type'])['filename'].apply(list).reset_index()

# Filter fields that have more than one unique Data Type
fields_with_diff_datatypes = grouped_df.groupby('Field').filter(lambda x: x['Data Type'].nunique() > 1)

# Write the results to S3 as a CSV file
with fs.open(s3_output_path, 'w') as f:
    fields_with_diff_datatypes.to_csv(f, index=False)

print("Fields with conflicting data types have been successfully saved to:", s3_output_path)











import pandas as pd
import s3fs
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import sys

# Initialize Spark and Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# S3 input and output paths
s3_input_path = 's3://datawarehouse-bilight/Staging/LOS3.0/bankVerificationDetailsCustom/Yearly/schemainfofolder/'  
s3_output_path = 's3://datawarehouse-bilight/Staging/LOS3.0/bankVerificationDetailsCustom/Yearly/schemainfofolder/output/fields_with_conflicting_data_types_1.csv'

# Initialize s3fs
fs = s3fs.S3FileSystem()

# Read all CSV files in the input S3 directory and merge them with filename tracking
merged_df = []
for file_path in fs.ls(s3_input_path):
    if file_path.endswith('.csv'):
        # Read each CSV file and add a 'filename' column
        df = pd.read_csv(f"s3://{file_path}", storage_options={"anon": False})
        df['filename'] = file_path.split('/')[-1]  # Extract filename
        merged_df.append(df)

# Concatenate all DataFrames
final_df = pd.concat(merged_df, ignore_index=True)

# Keep only the columns we need
final_df = final_df[['Field', 'Data Type', 'filename']]

# Pivot table to find missing Data Types
pivot_df = final_df.pivot_table(index='Field', columns='filename', values='Data Type', aggfunc='first').fillna("Data Type Not Present")

# Melt the pivot table back to a long format to list all data type occurrences
pivot_long_df = pivot_df.reset_index().melt(id_vars='Field', var_name='filename', value_name='Data Type')

# Group by Field and Data Type, aggregating filenames for each combination
grouped_df = pivot_long_df.groupby(['Field', 'Data Type'])['filename'].apply(list).reset_index()

# Filter for fields with inconsistent data types
fields_with_diff_datatypes = grouped_df.groupby('Field').filter(lambda x: x['Data Type'].nunique() > 1)

# Save the results with attractive formatting to S3
with fs.open(s3_output_path, 'w') as f:
    fields_with_diff_datatypes.to_csv(f, index=False)

print("Fields with conflicting data types have been successfully saved to:", s3_output_path)

