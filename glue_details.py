
##############################################################################################################

import boto3
import json
from datetime import datetime

# Initialize clients for Glue and S3
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

# Function to convert datetime objects to string
def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert to ISO 8601 format
    raise TypeError(f"Type {type(obj)} not serializable")

############################################# GLUE PART ###############################################

# Function to get all triggers
def get_all_triggers():
    triggers_response = glue_client.get_triggers()
    return triggers_response.get('Triggers', [])

# Function to get all workflows
def get_all_workflows():
    workflows_response = glue_client.list_workflows()
    workflows = workflows_response.get('Workflows', [])
    
    detailed_workflows = []
    for workflow in workflows:
        detailed_workflow = glue_client.get_workflow(Name=workflow)
        detailed_workflows.append(detailed_workflow['Workflow'])
    
    return detailed_workflows

# Function to get all jobs with optional prefix filtering
def get_glue_jobs(prefix=None):
    job_names = []
    next_token = None

    # Retrieve all job names with pagination
    while True:
        if next_token:
            response = glue_client.list_jobs(NextToken=next_token)
        else:
            response = glue_client.list_jobs()

        job_names.extend(response['JobNames'])
        next_token = response.get('NextToken')

        if not next_token:
            break

    jobs_metadata = []
    
    # Fetch detailed metadata for each job
    for job_name in job_names:
        job_details = glue_client.get_job(JobName=job_name)['Job']
        job_metadata = {
            "JobName": job_details['Name'],
            "Role": job_details.get('Role', 'N/A'),
            "CreatedOn": job_details.get('CreatedOn').strftime('%Y-%m-%d %H:%M:%S') if 'CreatedOn' in job_details else 'N/A',
            "LastModifiedOn": job_details.get('LastModifiedOn').strftime('%Y-%m-%d %H:%M:%S') if 'LastModifiedOn' in job_details else 'N/A',
            "ExecutionProperty": job_details.get('ExecutionProperty', {}).get('MaxConcurrentRuns', 'N/A'),
            "Command": {
                "ScriptLocation": job_details.get('Command', {}).get('ScriptLocation', 'N/A'),
                "PythonVersion": job_details.get('Command', {}).get('PythonVersion', 'N/A'),
            },
            "DefaultArguments": job_details.get('DefaultArguments', {}),
            "MaxRetries": job_details.get('MaxRetries', 'N/A'),
            "Timeout": job_details.get('Timeout', 'N/A'),
            "MaxCapacity": job_details.get('MaxCapacity', 'N/A'),
            "GlueVersion": job_details.get('GlueVersion', 'N/A')
        }
        
        jobs_metadata.append(job_metadata)

    # If a prefix is provided, filter job names that start with the prefix
    if prefix:
        filtered_jobs = [job for job in jobs_metadata if job['JobName'].startswith(prefix)]
        return filtered_jobs
    
    # Return all jobs metadata
    return jobs_metadata

########################################## S3 PART ################################################

# Function to get folder names inside a particular folder
def get_folders_in_s3(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    if 'CommonPrefixes' in response:
        folder_names = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        last_parts = [folder.split('/')[-2] for folder in folder_names]  # Extract just the folder name
        
        return {
            "FolderCount": len(last_parts),
            "Folders": last_parts
        }
    else:
        return {
            "FolderCount": 0,
            "Folders": []
        }

# Function to get file paths with metadata from a particular folder
def get_files_in_s3(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in response:
        files_info = []
        for item in response['Contents']:
            files_info.append({
                "FilePath": f"s3://{bucket_name}/{item['Key']}",
                "LastModified": item['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                "Size": item['Size'],  # Size in bytes
                "ETag": item['ETag'],  # Entity tag (hash of the file)
                "StorageClass": item['StorageClass'],  # Storage class of the file
                "Owner": item.get('Owner', {}).get('DisplayName', 'Unknown'),  # Owner of the file (if available)
                "VersionId": item.get('VersionId', 'N/A')  # Version ID (if versioning is enabled)
            })
        return {
            "FileCount": len(files_info),
            "Files": files_info
        }
    else:
        return {
            "FileCount": 0,
            "Files": []
        }

####################################### COMBINE EVERYTHING INTO ONE JSON #########################################

# Get Glue triggers, workflows, and jobs
triggers = get_all_triggers()
workflows = get_all_workflows()
jobs = get_glue_jobs()

# S3 bucket and prefix details
bucket_name = 'datawarehouse-bilight'
folder_prefix = 'Staging/LOS3.0/'
file_prefix = 'Staging/LOS3.0/dmDetails/Yearly/testing/'

# Get S3 folders and files metadata
folders_info = get_folders_in_s3(bucket_name, folder_prefix)
files_info = get_files_in_s3(bucket_name, file_prefix)

# Combine everything into a single JSON structure
combined_data = {
    "Glue": {
        "Triggers": triggers,
        "Workflows": workflows,
        "Jobs": jobs
    },
    "S3": {
        "Folders": folders_info,
        "Files": files_info
    }
}

# Print combined data as a JSON dump
print(json.dumps(combined_data, default=serialize, indent=4))
