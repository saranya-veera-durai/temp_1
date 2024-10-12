# # ##################################### CREATE WORKFLOW ###################################################


# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def create_workflow(glue_client, workflow_name, description=''):
#     try:
#         response = glue_client.create_workflow(
#             Name=workflow_name,
#             Description=description
#         )
#         print(f"Workflow '{workflow_name}' created successfully.")
#         return response
#     except glue_client.exceptions.AlreadyExistsException:
#         print(f"Workflow '{workflow_name}' already exists.")
#     except Exception as e:
#         print(f"Error creating workflow: {e}")
# # workflow_name = 'my_glue_workflow_1'
# # workflow_name = 'my_glue_workflow_2'
# # workflow_name = 'my_glue_workflow_3'
# workflow_name = 'master_workflow'

# create_workflow(glue_client, workflow_name, "My first Glue workflow")

# ###################################### CREATE SINGLE TRIGGER #################################################

# def create_trigger(glue_client, trigger_name, workflow_name, job_name, description='', actions=None, type='ON_DEMAND', predicate=None):
#     if actions is None:
#         actions = [{
#             'JobName': job_name
#         }]
#     if predicate is None:
#         predicate = {
#             'Logical': 'AND',
#             'Conditions': []
#         }

#     try:
#         response = glue_client.create_trigger(
#             Name=trigger_name,
#             Type=type,
#             Description=description,
#             Actions=actions,
#             StartOnCreation=False,
#             WorkflowName=workflow_name,
#             Predicate=predicate
#         )
#         print(f"Trigger '{trigger_name}' created successfully.")
#         return response
#     except glue_client.exceptions.AlreadyExistsException:
#         print(f"Trigger '{trigger_name}' already exists.")
#     except Exception as e:
#         print(f"Error creating trigger: {e}")
        
# trigger_name = 'start_master_workflow_trigger'
# job_name = "workflow_test"
# create_trigger(
#     glue_client,
#     trigger_name,
#     workflow_name,
#     job_name,
#     description='Trigger to start my Glue job on workflow start',
#     type='ON_DEMAND'  # Can be 'SCHEDULED' or 'CONDITIONAL'
# )

####################################### CREATE MULTIPLE TRIGGER #################################################
# def create_trigger(glue_client, trigger_name, workflow_name, job_names, description='', actions=None, type='ON_DEMAND', predicate=None):
#     """
#     Creates a Glue trigger that can initiate one or multiple Glue jobs.

#     Parameters:
#     - glue_client: Boto3 Glue client.
#     - trigger_name (str): Name of the trigger.
#     - workflow_name (str): Name of the workflow.
#     - job_names (str or list): Single job name or list of job names to trigger.
#     - description (str): Description of the trigger.
#     - actions (list, optional): List of action dictionaries. Overrides job_names if provided.
#     - type (str): Type of the trigger ('ON_DEMAND', 'SCHEDULED', 'CONDITIONAL').
#     - predicate (dict, optional): Predicate for conditional triggers.
#     """
#     # If actions are not provided, construct them from job_names
#     if actions is None:
#         if isinstance(job_names, list):
#             actions = [{'JobName': job} for job in job_names]
#         else:
#             actions = [{'JobName': job_names}]
    
#     # Only set a default predicate if it's a CONDITIONAL trigger
#     if predicate is None and type == 'CONDITIONAL':
#         predicate = {
#             'Logical': 'AND',  # Use 'AND' or 'ANY' based on your requirement
#             'Conditions': []
#         }

#     try:
#         kwargs = {
#             'Name': trigger_name,
#             'Type': type,
#             'Description': description,
#             'Actions': actions,
#             'StartOnCreation': False,
#             'WorkflowName': workflow_name
#         }
#         if predicate and type == 'CONDITIONAL':
#             kwargs['Predicate'] = predicate
#         response = glue_client.create_trigger(**kwargs)
#         print(f"Trigger '{trigger_name}' created successfully.")
#         return response
#     except glue_client.exceptions.AlreadyExistsException:
#         print(f"Trigger '{trigger_name}' already exists.")
#     except glue_client.exceptions.InvalidInputException as e:
#         print(f"Invalid input when creating trigger: {e}")
#     except Exception as e:
#         print(f"Error creating trigger: {e}")
        
        
# trigger_name = 'start_multiple_glue_jobs_trigger'
# job_names = ["LOS3.0_customerCreditDocCustom__customerCreditDetails_0", "glue_test_1", "glue_test_2",
# "1_LOS3.0_deviationMatrix-incremental","1_LOS3.0_appLifecycleLogs-incremental"]  # List of job names

# create_trigger(
#     glue_client=glue_client,
#     trigger_name=trigger_name,
#     workflow_name=workflow_name,
#     job_names=job_names,  # Pass the list of job names
#     description='Trigger to start multiple Glue jobs on workflow start',
#     type='ON_DEMAND'  # Can be 'SCHEDULED' or 'CONDITIONAL'
# )


# ###################################### CREATE OR UPDATE MULTIPLE TRIGGER ##################################################


# def create_or_update_trigger(
#     glue_client,
#     trigger_name,
#     workflow_name,
#     job_names,  # Accepts a single job name (str) or a list of job names (list)
#     description='',
#     actions=None,
#     type='ON_DEMAND',
#     predicate=None
# ):
#     """
#     Creates a new Glue trigger or updates an existing one with the specified configuration.

#     Parameters:
#     - glue_client: Boto3 Glue client.
#     - trigger_name (str): Name of the trigger.
#     - workflow_name (str): Name of the workflow.
#     - job_names (str or list): Single job name or list of job names to trigger.
#     - description (str): Description of the trigger.
#     - actions (list, optional): List of action dictionaries. Overrides job_names if provided.
#     - type (str): Type of the trigger ('ON_DEMAND', 'SCHEDULED', 'CONDITIONAL').
#     - predicate (dict, optional): Predicate for conditional triggers.

#     Returns:
#     - response (dict): Response from the AWS Glue API.
#     """
#     # If actions are not provided, construct them from job_names
#     if actions is None:
#         if isinstance(job_names, list):
#             actions = [{'JobName': job} for job in job_names]
#         else:
#             actions = [{'JobName': job_names}]

#     # Only set a default predicate if it's a CONDITIONAL trigger
#     if predicate is None and type == 'CONDITIONAL':
#         predicate = {
#             'Logical': 'AND',  # Use 'AND' or 'ANY' based on your requirement
#             'Conditions': []
#         }

#     try:
#         # Attempt to get the existing trigger
#         existing_trigger = glue_client.get_trigger(Name=trigger_name)
#         trigger_exists = True
#     except glue_client.exceptions.EntityNotFoundException:
#         trigger_exists = False
#     except Exception as e:
#         print(f"Error checking if trigger exists: {e}")
#         return None

#     if trigger_exists:
#         print(f"Trigger '{trigger_name}' already exists. Attempting to update it.")
#         try:
#             # Prepare the TriggerUpdate parameters
#             trigger_update = {
#                 'Description': description,
#                 'Actions': actions,
#             }

#             if predicate and type == 'CONDITIONAL':
#                 trigger_update['Predicate'] = predicate

#             # Update the trigger with TriggerUpdate dictionary
#             response = glue_client.update_trigger(
#                 Name=trigger_name,
#                 TriggerUpdate=trigger_update
#             )
#             print(f"Trigger '{trigger_name}' updated successfully.")
#             return response
#         except glue_client.exceptions.InvalidInputException as e:
#             print(f"Invalid input when updating trigger: {e}")
#             return None
#         except Exception as e:
#             print(f"Error updating trigger: {e}")
#             return None
#     else:
#         print(f"Trigger '{trigger_name}' does not exist. Creating a new one.")
#         try:
#             # Prepare the create parameters
#             create_params = {
#                 'Name': trigger_name,
#                 'Type': type,
#                 'Description': description,
#                 'Actions': actions,
#                 'StartOnCreation': False,  # Modify as needed
#                 'WorkflowName': workflow_name
#             }

#             if predicate and type == 'CONDITIONAL':
#                 create_params['Predicate'] = predicate

#             # Create the trigger
#             response = glue_client.create_trigger(**create_params)
#             print(f"Trigger '{trigger_name}' created successfully.")
#             return response
#         except glue_client.exceptions.AlreadyExistsException:
#             print(f"Trigger '{trigger_name}' already exists after all. It might have been created by another process.")
#         except glue_client.exceptions.InvalidInputException as e:
#             print(f"Invalid input when creating trigger: {e}")
#         except Exception as e:
#             print(f"Error creating trigger: {e}")
#             return None
            
# # trigger_name = 'start_multiple_glue_jobs_trigger_1'
# # job_names = [
# #     "main", 
# #     "LOS3.0_personalDiscussion__creditPd_activityLogs_0"
# # ]
# # trigger_name = 'start_multiple_glue_jobs_trigger_2'
# # job_names = [
# #     "LOS3.0_personalDiscussion__creditPdDetailsList_1", 
# #     "LOS3.0_personalDiscussion__creditPdDetailsList_personalDiscussionOutputList_2"
# # ]
# trigger_name = 'start_multiple_glue_jobs_trigger_3'
# job_names = [
#     "LOS3.0_personalDiscussion__salesPdDetailsList_3", 
#     "LOS3.0_personalDiscussion__salesPdDetailsList_personalDiscussionOutputList_4"
# ]
# # Ensure 'workflow_name' is defined
# # workflow_name = 'my_glue_workflow_1'
# # workflow_name = 'my_glue_workflow_2'
# workflow_name = 'my_glue_workflow_3'  # Replace with your actual workflow name

# create_or_update_trigger(
#     glue_client=glue_client,
#     trigger_name=trigger_name,
#     workflow_name=workflow_name,
#     job_names=job_names,  # Pass the list of job names
#     description='Trigger to start multiple Glue jobs on workflow start'
# )





# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%






# ##################################### CREATE MULTIPLE WORK FLOWS ##############################################

# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def create_workflow(glue_client, workflow_name, description=''):
#     try:
#         response = glue_client.create_workflow(
#             Name=workflow_name,
#             Description=description
#         )
#         print(f"Workflow '{workflow_name}' created successfully.")
#         return response
#     except glue_client.exceptions.AlreadyExistsException:
#         print(f"Workflow '{workflow_name}' already exists.")
#     except Exception as e:
#         print(f"Error creating workflow: {e}")

# # List of workflow names you want to create
# workflow_names = [
#     'my_glue_workflow_personalDiscussion',
#     #'my_glue_workflow_insurance'
# ]

# # Create workflows
# for workflow_name in workflow_names:
#     create_workflow(glue_client, workflow_name, f"Workflow for {workflow_name.replace('my_glue_workflow_', '')}")

###################################### TAKE JOBS LIST FOR TRIGGER GETTING JOBS NAMES ###########################################

# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def get_job_names(prefix):
#     """
#     Retrieve job names from AWS Glue that start with the given prefix.

#     Parameters:
#     - prefix (str): The prefix to filter job names.

#     Returns:
#     - list: A list of job names that match the prefix.
#     """
#     job_names = []
#     next_token = None

#     while True:
#         if next_token:
#             response = glue_client.list_jobs(NextToken=next_token)
#         else:
#             response = glue_client.list_jobs()

#         job_names.extend(response['JobNames'])
#         next_token = response.get('NextToken')

#         if not next_token:
#             break

#     # Filter job names based on the provided prefix
#     filtered_job_names = [job for job in job_names if job.startswith(prefix)]
#     return filtered_job_names

# def create_or_update_trigger(
#     glue_client,
#     trigger_name,
#     workflow_name,
#     job_names,
#     description='',
#     actions=None,
#     type='ON_DEMAND',
#     predicate=None
# ):
#     # If actions are not provided, construct them from job_names
#     if actions is None:
#         if isinstance(job_names, list):
#             actions = [{'JobName': job} for job in job_names]
#         else:
#             actions = [{'JobName': job_names}]

#     # Only set a default predicate if it's a CONDITIONAL trigger
#     if predicate is None and type == 'CONDITIONAL':
#         predicate = {
#             'Logical': 'AND',
#             'Conditions': []
#         }

#     try:
#         existing_trigger = glue_client.get_trigger(Name=trigger_name)
#         trigger_exists = True
#     except glue_client.exceptions.EntityNotFoundException:
#         trigger_exists = False
#     except Exception as e:
#         print(f"Error checking if trigger exists: {e}")
#         return None

#     if trigger_exists:
#         print(f"Trigger '{trigger_name}' already exists. Attempting to update it.")
#         try:
#             trigger_update = {
#                 'Description': description,
#                 'Actions': actions,
#             }

#             if predicate and type == 'CONDITIONAL':
#                 trigger_update['Predicate'] = predicate

#             response = glue_client.update_trigger(
#                 Name=trigger_name,
#                 TriggerUpdate=trigger_update
#             )
#             print(f"Trigger '{trigger_name}' updated successfully.")
#             return response
#         except glue_client.exceptions.InvalidInputException as e:
#             print(f"Invalid input when updating trigger: {e}")
#             return None
#         except Exception as e:
#             print(f"Error updating trigger: {e}")
#             return None
#     else:
#         print(f"Trigger '{trigger_name}' does not exist. Creating a new one.")
#         try:
#             create_params = {
#                 'Name': trigger_name,
#                 'Type': type,
#                 'Description': description,
#                 'Actions': actions,
#                 'StartOnCreation': False,  # Modify as needed
#                 'WorkflowName': workflow_name
#             }

#             if predicate and type == 'CONDITIONAL':
#                 create_params['Predicate'] = predicate

#             response = glue_client.create_trigger(**create_params)
#             print(f"Trigger '{trigger_name}' created successfully.")
#             return response
#         except glue_client.exceptions.AlreadyExistsException:
#             print(f"Trigger '{trigger_name}' already exists after all. It might have been created by another process.")
#         except glue_client.exceptions.InvalidInputException as e:
#             print(f"Invalid input when creating trigger: {e}")
#         except Exception as e:
#             print(f"Error creating trigger: {e}")
#             return None

# # Automate retrieval of job names for the specified prefixes
# personal_discussion_jobs = get_job_names("LOS3.0_personalDiscussion__")
# insurance_jobs = get_job_names("LOS3.0_Insurance__")

# # Create or update triggers for personal discussion jobs
# personal_discussion_trigger_name = 'start_multiple_glue_jobs_trigger_personalDiscussion'
# workflow_name = 'my_glue_workflow_personalDiscussion'  # Replace with your actual workflow name

# # create_or_update_trigger(
# #     glue_client=glue_client,
# #     trigger_name=personal_discussion_trigger_name,
# #     workflow_name=workflow_name,
# #     job_names=personal_discussion_jobs,
# #     description='Trigger to start multiple personal discussion Glue jobs on workflow start'
# # )

# # # Create or update triggers for insurance jobs
# # insurance_trigger_name = 'start_multiple_glue_jobs_trigger_insurance'
# # workflow_name = 'my_glue_workflow_insurance'  # Replace with your actual workflow name

# # create_or_update_trigger(
# #     glue_client=glue_client,
# #     trigger_name=insurance_trigger_name,
# #     workflow_name=workflow_name,
# #     job_names=insurance_jobs,
# #     description='Trigger to start multiple insurance Glue jobs on workflow start'
# # )



###################################### TRIGGER MULTIPLE WORKFLOWS BY RUNNING THIS CODE ################################################

# import boto3

# # Initialize the Glue client for your region
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your AWS region

# def trigger_workflow(glue_client, workflow_name):
#     """
#     Function to trigger a Glue workflow.
    
#     Parameters:
#     - glue_client: Boto3 Glue client.
#     - workflow_name (str): Name of the workflow to trigger.

#     Returns:
#     - None: Triggers the workflow and prints the result.
#     """
#     try:
#         # Start the workflow using the StartWorkflowRun API
#         response = glue_client.start_workflow_run(Name=workflow_name)
#         print(f"Workflow '{workflow_name}' started successfully with RunId: {response['RunId']}")
#     except Exception as e:
#         print(f"Error starting workflow '{workflow_name}': {e}")

# # List of workflow names you want to trigger
# workflows_to_trigger = [
#     "my_glue_workflow_1",  
#     "my_glue_workflow_2",  
#     "my_glue_workflow_3",
#     'my_glue_workflow_personalDiscussion',
#     #etc.,
# ]

# Loop over each workflow and trigger it
for workflow in workflows_to_trigger:
    trigger_workflow(glue_client, workflow)

print("All workflows have been triggered.")

