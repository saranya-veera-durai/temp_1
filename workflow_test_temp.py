# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def create_workflow(workflow_name, description=''):
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

# # Create the workflow
# workflow_name = 'Insurance_Workflow'
# create_workflow(workflow_name, f"Workflow for {workflow_name}")

# def create_or_update_trigger(trigger_name, job_names, workflow_name="Insurance_Workflow", previous_job_name=None):
#     try:
#         # Check if the trigger already exists
#         try:
#             existing_trigger_response = glue_client.get_trigger(Name=trigger_name)
#             existing_trigger = existing_trigger_response['Trigger']
#             print(f"Trigger '{trigger_name}' already exists, updating it.")
#             print(f"{existing_trigger}")
            
#             # Prepare the new list of jobs (update with the modified jobs)
#             new_actions = [{
#                 'JobName': job,
#                 'Arguments': {}
#             } for job in job_names]

#             # Prepare predicate only if it's a conditional trigger
#             predicate = None
#             if previous_job_name:
#                 predicate = {
#                     'Logical': 'AND',
#                     'Conditions': [{
#                         'LogicalOperator': 'EQUALS',
#                         'JobName': previous_job_name,
#                         'State': 'SUCCEEDED'
#                     }]
#                 }

#             trigger_type = existing_trigger.get('Type', 'ON_DEMAND')  # Default to 'ON_DEMAND' if not found

#             if trigger_type == 'ON_DEMAND':
#                 print("Updating an ON_DEMAND trigger.")
#                 trigger_response = glue_client.update_trigger(
#                     Name=trigger_name,
#                     TriggerUpdate={
#                         'Name': trigger_name,
#                         'Actions': new_actions,
#                         # 'Predicate': predicate  # Typically not used for ON_DEMAND
#                     }
#                 )
#             elif trigger_type == 'CONDITIONAL':
#                 print("Updating a CONDITIONAL trigger.")
#                 trigger_response = glue_client.update_trigger(
#                     Name=trigger_name,
#                     TriggerUpdate={
#                         'Name': trigger_name,
#                         'Actions': new_actions,
#                         'Predicate': predicate  # Add predicate for conditional triggers
#                     }
#                 )
#             else:
#                 print(f"Unsupported trigger type: {trigger_type}")
#                 return None

#             print(f"Trigger '{trigger_name}' updated successfully.")
#         except glue_client.exceptions.EntityNotFoundException:
#             # Trigger doesn't exist, so create a new one
#             print(f"Trigger '{trigger_name}' not found, creating it.")

#             # Create a new trigger
#             if previous_job_name:
#                 # Create a conditional trigger that runs after the previous job completes
#                 trigger_response = glue_client.create_trigger(
#                     Name=trigger_name,
#                     Type='CONDITIONAL',
#                     Actions=[{
#                         'JobName': job,
#                         'Arguments': {}
#                     } for job in job_names],
#                     WorkflowName=workflow_name,
#                     Predicate={
#                         'Logical': 'AND',
#                         'Conditions': [{
#                             'LogicalOperator': 'EQUALS',
#                             'JobName': previous_job_name,
#                             'State': 'SUCCEEDED'
#                         }]
#                     },
#                     StartOnCreation=False  # Do not auto-activate the trigger upon creation
#                 )
#             else:
#                 # Create an on-demand trigger
#                 trigger_response = glue_client.create_trigger(
#                     Name=trigger_name,
#                     Type='ON_DEMAND',
#                     Actions=[{
#                         'JobName': job,
#                         'Arguments': {}
#                     } for job in job_names],
#                     WorkflowName=workflow_name,
#                     StartOnCreation=False
#                 )

#             print(f"Trigger '{trigger_name}' created successfully.")

#         return trigger_response
#     except Exception as e:
#         print(f"Error creating or updating trigger '{trigger_name}': {e}")
#         return None

# def activate_trigger(trigger_name):
#     try:
#         response = glue_client.start_trigger(Name=trigger_name)  # Explicitly activating the trigger
#         print(f"Trigger '{trigger_name}' activated successfully.")
#         return response
#     except Exception as e:
#         print(f"Error activating trigger '{trigger_name}': {e}")
#         return None

# # Define trigger names and job names
# triggers_and_jobs = {
#     "sample_new_trigger_1": [
#         "LOS3.0_insuranceData__applicantList_phone_20",
#         "LOS3.0_insuranceData__applicantList_kyc_19"
#     ],
#     "sample_new_trigger_2": [
#         "LOS3.0_insuranceData__applicantList_employment_18",
#         "LOS3.0_insuranceData__applicantList_email_17"
#     ],
#     "sample_new_trigger_3": [
#         "LOS3.0_insuranceData__applicantList_customObject_aIMD2_16",
#         "LOS3.0_insuranceData__applicantList_customObject_aActivityLog_15",
#         "LOS3.0_insuranceData__applicantList_customObject_InsurancePolicy_oAddendum_aQuestionnaire_aMember_14"
#     ],
#     "sample_new_trigger_4": [
#         "LOS3.0_insuranceData__applicantList_contactPersonDetailsList_5",    
#         "LOS3.0_insuranceData__applicantList_businessDetails_gstDetails_4",
#     ]
# }

# # Function to create and activate triggers in sequence
# def create_and_activate_triggers(triggers_and_jobs):
#     previous_job_name = None

#     for trigger_name, job_names in triggers_and_jobs.items():
#         # Create the trigger
#         trigger_response = create_or_update_trigger(trigger_name, job_names, previous_job_name=previous_job_name)
        
#         if trigger_response:
#             # Activate the trigger
#             activate_trigger(trigger_name)
#             previous_job_name = job_names[-1]  # Update previous job name for the next trigger
#         else:
#             print(f"Failed to create the trigger: {trigger_name}")
#             break  # Exit on failure

# # Execute the function
# create_and_activate_triggers(triggers_and_jobs)

# # # Trigger names
# # trigger_name_1 = "sample_new_trigger_1"
# # trigger_name_2 = "sample_new_trigger_2"
# # trigger_name_3 = "sample_new_trigger_3"
# # trigger_name_4 = "sample_new_trigger_4"
# # trigger_name_5 = "sample_new_trigger_5"

# # # Job names for each trigger
# # job_names_1 = [
# #     "LOS3.0_insuranceData__applicantList_phone_20",
# # ]
# # job_names_2 = [
# #     "LOS3.0_insuranceData__applicantList_employment_18",
# #     "LOS3.0_insuranceData__applicantList_email_17",
# #     "LOS3.0_insuranceData__applicantList_kyc_19"
# # ]
# # job_names_3 = [
# #     "LOS3.0_insuranceData__applicantList_customObject_aIMD2_16",
# #     "LOS3.0_insuranceData__applicantList_customObject_aActivityLog_15",
# #     "LOS3.0_insuranceData__applicantList_customObject_InsurancePolicy_oAddendum_aQuestionnaire_aMember_14"
# # ]
# # job_names_4 = [
# #     "LOS3.0_insuranceData__applicantList_contactPersonDetailsList_5",	
# #     "LOS3.0_insuranceData__applicantList_businessDetails_gstDetails_4",
# #     "LOS3.0_insuranceData__applicantList_address_3"
# # ]	

# # job_names_5 = [
# #     "LOS3.0_insuranceData__applicantList_contactPersonDetailsList_phone_8"
# # ]

# # # Create the first trigger (on-demand, no auto-activation)
# # trigger_response_1 = create_or_update_trigger(trigger_name_1, job_names_1)

# # # Activate the first trigger explicitly
# # if trigger_response_1:
# #     activate_trigger(trigger_name_1)
# #     last_job_of_first_trigger = job_names_1[-1]  # Get the last job name from the first trigger

# #     # Create the second trigger (conditional on-demand, no auto-activation)
# #     trigger_response_2 = create_or_update_trigger(trigger_name_2, job_names_2, previous_job_name=last_job_of_first_trigger)

# #     if trigger_response_2:
# #         # Automatically activate the second trigger after creation
# #         activate_trigger(trigger_name_2)
# #         last_job_of_second_trigger = job_names_2[-1]  # Get the last job name from the second trigger

# #         # Create the third trigger (conditional on-demand, no auto-activation)
# #         trigger_response_3 = create_or_update_trigger(trigger_name_3, job_names_3, previous_job_name=last_job_of_second_trigger)

# #         if trigger_response_3:
# #             # Automatically activate the third trigger after creation
# #             activate_trigger(trigger_name_3)
# #             last_job_of_third_trigger = job_names_3[-1]  
# #             print("All triggers up to the third one created and activated successfully.")
            
# #             # Create the fourth trigger
# #             trigger_response_4 = create_or_update_trigger(trigger_name_4, job_names_4, previous_job_name=last_job_of_third_trigger)

# #             if trigger_response_4:
# #                 # Automatically activate the fourth trigger after creation
# #                 activate_trigger(trigger_name_4)
# #                 ast_job_of_four_trigger = job_names_4[-1]  
# #                 trigger_response_5 = create_or_update_trigger(trigger_name_5, job_names_5, previous_job_name=ast_job_of_four_trigger)

# #                 if trigger_response_5:
# #                     # Automatically activate the fourth trigger after creation
# #                     activate_trigger(trigger_name_5)
# #                 else:
# #                     print("Failed to create the fourth trigger.")
# #             else:
# #                 print("Failed to create the fourth trigger.")
# #         else:
# #             print("Failed to create the third trigger.")
# #     else:
# #         print("Failed to create the second trigger.")
# # else:
# #     print("Failed to create the first trigger.")


#########################################################################################################################################

# import boto3
# import datetime
# import json  # Import the json module

# # Initialize boto3 clients
# glue_client = boto3.client('glue')
# logs_client = boto3.client('logs')

# def get_job_info(givenjobname):
#     response = glue_client.get_job_runs(JobName=str(givenjobname))
#     if response["JobRuns"] == []:
#         try:
#             NumberOfWorkers = i["NumberOfWorkers"]
#         except:
#             NumberOfWorkers = ""
#         try:
#             WorkerType = i["WorkerType"]
#         except:
#             WorkerType = ""
#         return {
#             "jobname": givenjobname,
#             "NumberOfWorkers" :NumberOfWorkers,
#             "ExecutionTime" :"",
#             "WorkerType" :WorkerType,
#             "Status":"",
#             "StartedOn": "",
#             "LastModifiedOn": "",
#             "CompletedOn": "",
#             "ErrorMessage": "",
#             "date": response["ResponseMetadata"]["HTTPHeaders"]["date"]         
#         }
#     else:    
#         for i in response["JobRuns"]:
#             try:
#                 NumberOfWorkers = i["NumberOfWorkers"]
#             except:
#                 NumberOfWorkers = ""
#             try:
#                 ExecutionTime = i["ExecutionTime"]
#             except:
#                 ExecutionTime = ""
#             try:
#                 WorkerType = i["WorkerType"]
#             except:
#                 WorkerType = ""
#             try:
#                 run_status = i["JobRunState"]
#             except:
#                 run_status = ""
#             jobname = i["JobName"]
#             LastModifiedOn = i["LastModifiedOn"]
#             try:
#                 CompletedOn = i["CompletedOn"]
#             except:
#                 CompletedOn = ""
#             StartedOn = i["StartedOn"]
#             try:
#                 ErrorMessage = i["ErrorMessage"]
#             except:
#                 ErrorMessage = ""
#             return {
#                 "jobname": jobname,            
#                 "NumberOfWorkers" :NumberOfWorkers,
#                 "ExecutionTime" :ExecutionTime,
#                 "WorkerType" :WorkerType,
#                 "Status":run_status,
#                 "StartedOn": str(StartedOn),
#                 "LastModifiedOn": str(LastModifiedOn),
#                 "CompletedOn": str(CompletedOn),
#                 "ErrorMessage": ErrorMessage,
#                 "date": response["ResponseMetadata"]["HTTPHeaders"]["date"]
#             }

# res = glue_client.list_jobs()
# jobname = []
# next_token = None
# while True:
#     for job in res['JobNames']:
#         jobname.append(job)
#     next_token = res.get('NextToken')
#     if not next_token:
#         break
#     res = glue_client.list_jobs(NextToken=next_token)

# all_job_dis = []
# insuranceData = []
# camdetails = []
# for i in jobname:
#     all_job_dis.append(get_job_info(i))
#     if "LOS3.0_insuranceData__" in i:
#         insuranceData.append(i)
#     elif "LOS3.0_camdetails" in i:
#         camdetails.append(i)
        
# # Print the output as JSON
# #print(json.dumps(all_job_dis, indent=4, default=str))  # Use json.dumps for pretty printing
# #print(insuranceData)
# #print("###########################")
# #print(camdetails)
# #print("###########################")

# print(len(insuranceData))
# chunk_size = 30
# sublists = [insuranceData[i:i+ chunk_size] for i in range(0, len(insuranceData), chunk_size)]

# for index, sublist in enumerate(sublists):
#     print("--------------------")
#     chunk_size_nested = 3
#     next_sub_lists = [sublist[i:i+ chunk_size_nested] for i in range(0, len(sublist), chunk_size_nested)]
#     for i in next_sub_lists:
#         print("########################")
#         print(i)

################################################################# GET JOBS LIST #############################################################


# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def get_job_names(prefix):
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


# # Automate retrieval of job names for the specified prefixes
# insurance_jobs = get_job_names("LOS3.0_insuranceData__")
# print(insurance_jobs)
# insuranceData=list(insurance_jobs)

# print(len(insuranceData))
# chunk_size = 30
# sublists = [insuranceData[i:i+ chunk_size] for i in range(0, len(insuranceData), chunk_size)]

# for index, sublist in enumerate(sublists):
#     print("--------------------")
#     chunk_size_nested = 3
#     next_sub_lists = [sublist[i:i+ chunk_size_nested] for i in range(0, len(sublist), chunk_size_nested)]
#     for i in next_sub_lists:
#         print("########################")
#         print(i)


# ##################### CREATE DYNAMIC WORKFLOWS WITH 10 TRIGGERS WITH 3 JOBS ################################
# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def get_job_names(prefix):
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

# # Automate retrieval of job names for the specified prefixes
# insurance_jobs = get_job_names("LOS3.0_insuranceData__")
# insuranceData = list(insurance_jobs)

# # Divide jobs into sublists of size 3
# chunk_size_nested = 3
# sublists = [insuranceData[i:i + chunk_size_nested] for i in range(0, len(insuranceData), chunk_size_nested)]

# # Function to create workflows dynamically
# def create_workflow(workflow_name, description=''):
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

# def activate_trigger(trigger_name):
#     try:
#         response = glue_client.start_trigger(Name=trigger_name)
#         print(f"Trigger '{trigger_name}' activated successfully.")
#         return response
#     except Exception as e:
#         print(f"Error activating trigger '{trigger_name}': {e}")
#         return None

# # Function to create and activate triggers in sequence
# def create_and_activate_triggers(sublists, workflow_name):
#     previous_job_name = None

#     for i, job_names in enumerate(sublists):
#         trigger_name = f"{workflow_name}_trigger_{i+1}"  # Dynamic trigger name
#         trigger_response = create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=previous_job_name)
        
#         if trigger_response:
#             activate_trigger(trigger_name)
#             previous_job_name = job_names[-1]  # Last job of the current trigger to chain the next trigger
#         else:
#             print(f"Failed to create the trigger: {trigger_name}")
#             break

# # Main function to manage workflows and triggers
# def manage_workflows_and_triggers(sublists):
#     # Each workflow can handle up to 30 jobs (10 triggers with 3 jobs each)
#     max_jobs_per_workflow = 15
#     workflow_count = 1

#     for i in range(0, len(sublists), 5):  # 10 triggers per workflow, each trigger has 3 jobs
#         workflow_sublists = sublists[i:i + 5]  # Get a chunk of 10 triggers (30 jobs)
        
#         workflow_name = f"Insurance_Workflow_{workflow_count}"
#         create_workflow(workflow_name, f"Workflow for {workflow_name}")
        
#         create_and_activate_triggers(workflow_sublists, workflow_name)
        
#         workflow_count += 1

# # Execute the function to manage workflows and triggers
# manage_workflows_and_triggers(sublists)


############################ total no of workflows and triggers ############################

# import boto3
# import json
# from datetime import datetime

# # Create a Glue client
# glue_client = boto3.client('glue')

# # Function to convert datetime objects to string
# def serialize(obj):
#     if isinstance(obj, datetime):
#         return obj.isoformat()  # Convert to ISO 8601 format
#     raise TypeError(f"Type {type(obj)} not serializable")

# # Function to get all triggers
# def get_all_triggers():
#     triggers_response = glue_client.get_triggers()
#     return triggers_response.get('Triggers', [])

# # Function to get all workflows
# def get_all_workflows():
#     workflows_response = glue_client.list_workflows()
#     workflows = workflows_response.get('Workflows', [])
    
#     detailed_workflows = []
#     for workflow in workflows:
#         detailed_workflow = glue_client.get_workflow(Name=workflow)
#         detailed_workflows.append(detailed_workflow['Workflow'])
    
#     return detailed_workflows

# # Get triggers and workflows
# triggers = get_all_triggers()
# workflows = get_all_workflows()

# # Combine into a single dictionary
# output = {
#     'Triggers': triggers,
#     'Workflows': workflows
# }

# # Print as a JSON dump
# print(json.dumps(output, default=serialize, indent=4))

############################################# ACTIVATED ALL TRIGGER EXCEPT 1ST ONE ############################################################
# import boto3

# # Initialize the Glue client
# glue_client = boto3.client('glue', region_name='ap-south-1')  # Replace with your region

# def get_job_names(prefix):
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

# # Automate retrieval of job names for the specified prefixes
# insurance_jobs = get_job_names("LOS3.0_insuranceData__")
# insuranceData = list(insurance_jobs)

# # Divide jobs into sublists of size 3
# chunk_size_nested = 3
# sublists = [insuranceData[i:i + chunk_size_nested] for i in range(0, len(insuranceData), chunk_size_nested)]

# # Function to create workflows dynamically
# def create_workflow(workflow_name, description=''):
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

# def create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=None, trigger_type='CONDITIONAL'):
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
#                     Type=trigger_type,  # Use the passed trigger_type
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
#                     Type=trigger_type,  # Use the passed trigger_type
#                     Actions=[{'JobName': job} for job in job_names],
#                     WorkflowName=workflow_name,
#                     StartOnCreation=False
#                 )
#             print(f"Trigger '{trigger_name}' created successfully.")
#         return trigger_response
#     except Exception as e:
#         print(f"Error creating or updating trigger '{trigger_name}': {e}")
#         return None

# def activate_trigger(trigger_name):
#     try:
#         response = glue_client.start_trigger(Name=trigger_name)
#         print(f"Trigger '{trigger_name}' activated successfully.")
#         return response
#     except Exception as e:
#         print(f"Error activating trigger '{trigger_name}': {e}")
#         return None

# # Function to create and activate triggers in sequence
# def create_and_activate_triggers(sublists, workflow_name, is_first_workflow, previous_job_name=None):
#     for i, job_names in enumerate(sublists):
#         trigger_name = f"{workflow_name}_trigger_{i + 1}"  # Dynamic trigger name
        
#         # Determine trigger type based on workflow and trigger index
#         trigger_type = 'ON_DEMAND' if is_first_workflow and i == 0 else 'CONDITIONAL'

#         trigger_response = create_or_update_trigger(trigger_name, job_names, workflow_name, previous_job_name=previous_job_name, trigger_type=trigger_type)
        
#         if trigger_response:
#             activate_trigger(trigger_name)
#             previous_job_name = job_names[-1]  # Last job of the current trigger to chain the next trigger
#         else:
#             print(f"Failed to create the trigger: {trigger_name}")
#             break

# # Main function to manage workflows and triggers
# def manage_workflows_and_triggers(sublists):
#     # Each workflow can handle up to 30 jobs (10 triggers with 3 jobs each)
#     max_jobs_per_workflow = 6
#     workflow_count = 1

#     for i in range(0, len(sublists), 2):  # 10 triggers per workflow, each trigger has 3 jobs
#         workflow_sublists = sublists[i:i + 2]  # Get a chunk of 10 triggers (30 jobs)
        
#         workflow_name = f"Insurance_Workflow_{workflow_count}"
#         create_workflow(workflow_name, f"Workflow for {workflow_name}")
        
#         # Pass True for the first workflow to trigger the ON_DEMAND condition
#         previous_job_name = None if workflow_count == 1 else f"{workflow_name}_trigger_{i - 1}"
#         create_and_activate_triggers(workflow_sublists, workflow_name, is_first_workflow=(workflow_count == 1), previous_job_name=previous_job_name)
        
#         workflow_count += 1

# # Execute the function to manage workflows and triggers
# manage_workflows_and_triggers(sublists)


########################################################################################################################
