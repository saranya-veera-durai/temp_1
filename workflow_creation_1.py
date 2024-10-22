####################################### Needed Input ######################################################

file_parent_name = "sanctionConditions"

##################### CREATE DYNAMIC WORKFLOWS WITH 2 TRIGGERS WITH 3 JOBS ################################
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
job_prefix = f"LOS3.0_{file_parent_name}__"  # Dynamically build the prefix based on file_parent_name
file_parent_name_jobs = get_job_names(job_prefix)  # Retrieve job names based on the dynamically constructed prefix
file_parent_name_list = list(file_parent_name_jobs)  # Convert job names to a list

# Divide jobs into sublists of size 3
chunk_size_nested = 3
sublists = [file_parent_name_list[i:i + chunk_size_nested] for i in range(0, len(file_parent_name_list), chunk_size_nested)]

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


# def create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=None):
#     try:
#         # Check if the trigger already exists
#         try:
#             existing_trigger_response = glue_client.get_trigger(Name=trigger_name)
#             existing_trigger = existing_trigger_response['Trigger']
#             print(f"Trigger '{trigger_name}' already exists, updating it.")
            
#             new_actions = [{'JobName': job} for job in job_names]
#             predicate = None
#             if previous_job_name:
#                 predicate = {
#                     'Logical': 'AND',
#                     'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': previous_job_name, 'State': 'SUCCEEDED'}]
#                 }
#             trigger_response = glue_client.update_trigger(
#                 Name=trigger_name,
#                 TriggerUpdate={
#                     'Name': trigger_name,
#                     'Actions': new_actions,
#                     'Predicate': predicate
#                 }
#             )
#             print(f"Trigger '{trigger_name}' updated successfully.")
#         except glue_client.exceptions.EntityNotFoundException:
#             print(f"Trigger '{trigger_name}' not found, creating it.")
#             if previous_job_name:
#                 trigger_response = glue_client.create_trigger(
#                     Name=trigger_name,
#                     Type='CONDITIONAL',
#                     Actions=[{'JobName': job} for job in job_names],
#                     WorkflowName=workflow_name,
#                     Predicate={
#                         'Logical': 'AND',
#                         'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': previous_job_name, 'State': 'SUCCEEDED'}]
#                     },
#                     StartOnCreation=False
#                 )
#             else:
#                 trigger_response = glue_client.create_trigger(
#                     Name=trigger_name,
#                     Type='ON_DEMAND',
#                     Actions=[{'JobName': job} for job in job_names],
#                     WorkflowName=workflow_name,
#                     StartOnCreation=False
#                 )
#             print(f"Trigger '{trigger_name}' created successfully.")
#         return trigger_response
#     except Exception as e:
#         print(f"Error creating or updating trigger '{trigger_name}': {e}")
#         return None
def create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=None):
    try:
        # Check if the trigger already exists
        try:
            existing_trigger_response = glue_client.get_trigger(Name=trigger_name)
            existing_trigger = existing_trigger_response['Trigger']
            print(f"Trigger '{trigger_name}' already exists, updating it.")

            new_actions = [{'JobName': job} for job in job_names]
            trigger_update_params = {
                'Name': trigger_name,
                'Actions': new_actions,
            }

            if previous_job_name:
                trigger_update_params['Predicate'] = {
                    'Logical': 'AND',
                    'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': previous_job_name, 'State': 'SUCCEEDED'}]
                }
            else:
                trigger_update_params['Predicate'] = None  # No predicate if no previous job

            trigger_response = glue_client.update_trigger(
                Name=trigger_name,
                TriggerUpdate=trigger_update_params
            )
            print(f"Trigger '{trigger_name}' updated successfully.")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"Trigger '{trigger_name}' not found, creating it.")
            trigger_create_params = {
                'Name': trigger_name,
                'Actions': [{'JobName': job} for job in job_names],
                'WorkflowName': workflow_name,
                'StartOnCreation': False
            }

            if previous_job_name:
                trigger_create_params['Type'] = 'CONDITIONAL'
                trigger_create_params['Predicate'] = {
                    'Logical': 'AND',
                    'Conditions': [{'LogicalOperator': 'EQUALS', 'JobName': previous_job_name, 'State': 'SUCCEEDED'}]
                }
            else:
                trigger_create_params['Type'] = 'ON_DEMAND'

            trigger_response = glue_client.create_trigger(**trigger_create_params)
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

# Function to create and activate triggers in sequence
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

# Main function to manage workflows and triggers
def manage_workflows_and_triggers(sublists,file_parent_name):
    # Each workflow can handle up to 30 jobs (10 triggers with 3 jobs each)
    max_jobs_per_workflow = 6
    workflow_count = 1

    for i in range(0, len(sublists), 2):  # 10 triggers per workflow, each trigger has 3 jobs
        workflow_sublists = sublists[i:i + 2]  # Get a chunk of 10 triggers (30 jobs)
        
        workflow_name = f"{file_parent_name}_Workflow_{workflow_count}"
        create_workflow(workflow_name, f"Workflow for {workflow_name}")
        
        create_and_activate_triggers(workflow_sublists, workflow_name)
        
        workflow_count += 1

# Execute the function to manage workflows and triggers
manage_workflows_and_triggers(sublists,file_parent_name)



############################################# CONNECT ONE WORK FLOW TO NEXT WORK FLOW ##############################################
import boto3

# Create a Glue client
glue_client = boto3.client('glue')

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

# Function to find the last job of each workflow
def get_last_jobs_of_workflows(workflows):
    last_jobs = {}
    
    for workflow in workflows:
        workflow_name = workflow['Name']
        last_job = None
        
        # Get all triggers associated with the workflow
        triggers = get_all_triggers()
        
        for trigger in triggers:
            if trigger['WorkflowName'] == workflow_name:
                # Get the actions of the trigger
                actions = trigger.get('Actions', [])
                
                # Update the last job with the last action's job name
                if actions:
                    last_job = actions[-1]['JobName']  # Get the last job in the actions list
        
        last_jobs[workflow_name] = last_job if last_job else 'No jobs found'
    
    return last_jobs

# Get all workflows
workflows = get_all_workflows()

# Find last jobs for each workflow
last_jobs = get_last_jobs_of_workflows(workflows)

# List of workflows in order (this can be customized)
workflow_order = [workflow['Name'] for workflow in workflows]

# Automate the S3 path generation based on last job names
bucket_name = 'datawarehouse-bilight'
s3_prefix = f'Staging/LOS3.0/{file_parent_name}/Yearly/demo/'

job_s3_paths = {}
for workflow_name, last_job_name in last_jobs.items():
    if last_job_name != 'No jobs found':
        # Construct the S3 path based on the last job name
        script_path = f's3://{bucket_name}/{s3_prefix}{last_job_name}.py'
        job_s3_paths[last_job_name] = script_path

# Create an S3 client
s3 = boto3.client('s3')

for job_name, script_path in job_s3_paths.items():
    try:
        # Parse the S3 path
        bucket, key = script_path.replace('s3://', '').split('/', 1)
        
        # Download the script
        s3.download_file(bucket, key, f"{job_name}.py")

        # Determine the current workflow of the job
        current_workflow_name = None
        for workflow, last_job in last_jobs.items():
            if last_job == job_name:
                current_workflow_name = workflow
                break
        
        # Determine the next workflow name
        next_workflow_name = None
        if current_workflow_name:
            try:
                current_index = workflow_order.index(current_workflow_name)
                # Get the next workflow; check if it's not the last in the list
                if current_index < len(workflow_order) - 1:
                    next_workflow_name = workflow_order[current_index + 1]
                else:
                    print(f"No next workflow for the last job '{current_workflow_name}'.")
            except ValueError:
                print(f"Warning: {current_workflow_name} not found in workflow order.")

        # If a next workflow is found, create a custom message
        if next_workflow_name:
            thank_you_message = f"""\
############################# START NEXT WORK FLOW ########################
import sys
from awsglue.utils import getResolvedOptions

workflow_name = "{next_workflow_name}"

import boto3
glue_client = boto3.client('glue', region_name='ap-south-1')

# Start the next workflow job, passing the workflow_name as an argument
response = glue_client.start_job_run(
    JobName='workflow_run_connection_1',  # Name of the next Glue job
    Arguments={{
        '--workflow_name': workflow_name  # Pass workflow_name as a parameter
    }}
)

"""

            # Append the thank you message
            with open(f"{job_name}.py", 'a') as file:
                file.write(f"\n{thank_you_message}")

            # Upload the modified script back to S3
            s3.upload_file(f"{job_name}.py", bucket, key)

            print(f"Appended message to {job_name} job with next workflow '{next_workflow_name}'.")

        else:
            print(f"No next workflow found for job {job_name}. Skipping.")

    except Exception as e:
        print(f"Error processing job {job_name}: {e}")
