####################################################### Needed Input #####################################################################

file_parent_name = "sanctionConditions"

####################################### To get S3 Paths for particular folder ############################################################

import boto3
s3_client = boto3.client('s3')
bucket_name = 'datawarehouse-bilight'

def generate_table_name(file_name):
    base_name = file_name.split('.')[0]  # Get the part before the extension
    return base_name.lower()  # Convert to lowercase for consistency

files_and_tables_list = []
try:
    # Set the dynamic prefix based on the folder name
    prefix = f'Staging/LOS3.0/{file_parent_name}/Yearly/demo/'  
    
    # List objects in the specified folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Check if there are contents in the response and list file paths
    if 'Contents' in response:
        for item in response['Contents']:
            s3_key = item['Key']

            # Skip folder entries (those that end with '/') and non-CSV files
            if s3_key.endswith('/') or not s3_key.endswith('.csv'):
                continue
            
            file_name = s3_key.split('/')[-1]  # Extract the file name from the path
            
            # Skip 'schema_info.csv'
            if file_name == 'schema_info.csv':
                continue
            
            # Build the S3 path
            s3_path = f"s3://{bucket_name}/{s3_key}"
            
            # #Derive the table name from the file name
            # table_name = f"{file_parent_name.lower()}.{generate_table_name(file_name)}"
            table_name = f"deviationmatrix.{generate_table_name(file_name)}"

            # Add to the list as a dictionary with s3_path and table_name
            files_and_tables_list.append({
                "s3_path": s3_path,
                "table_name": table_name
            })
    else:
        print(f"No files found in the folder: {file_parent_name}")

except Exception as e:
    # Print error if something goes wrong
    print(f"Error processing folder '{file_parent_name}': {str(e)}")
print(files_and_tables_list)



# ######################################## Datas send from s3 to redshift ######################################################



import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()                                                                                                                         
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

############################################################################################################################
########################################### List of CSV files and their corresponding Redshift table #######################
############################################################################################################################

files_and_tables = files_and_tables_list
############################################################################################################################
########################################### Process each file and write to Redshift ########################################
############################################################################################################################

for file_info in files_and_tables:
    # Read the CSV file into a DynamicFrame
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "nullValue": ""},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [file_info["s3_path"]], "recurse": True},
        transformation_ctx=f"AmazonS3_{file_info['table_name']}"
    )
    # Transform the DynamicFrame to replace empty strings with None
    transformed_df = dynamic_frame.toDF().na.replace('', None)  # Replace empty strings with None
    transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")

    # Write the DynamicFrame to Redshift
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamic_frame,
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": "s3://aws-glue-assets-493316969816-ap-south-1/temporary/",
            "useConnectionProperties": "true",
            "dbtable": file_info["table_name"],
            "connectionName": "Redshift connection New Latest"
        },
        transformation_ctx=f"AmazonRedshift_{file_info['table_name']}"
    )

job.commit()

