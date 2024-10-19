import boto3
import sys
from awsglue.utils import getResolvedOptions
# Initialize the Glue client
glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# List all available workflows
response = glue_client.list_workflows()
workflows = response.get('Workflows', [])
print("Available workflows:", workflows)


# Retrieve the passed parameters (workflow_name in this case)
args = getResolvedOptions(sys.argv, ['workflow_name'])

workflow_name = args['workflow_name']
workflow_details = glue_client.get_workflow(Name=workflow_name)
print("Workflow Details:", workflow_details)

workflow_run_response = glue_client.start_workflow_run(Name=workflow_name)
workflow_run_id = workflow_run_response['RunId']
print("Started Workflow Run with ID:", workflow_run_id)
