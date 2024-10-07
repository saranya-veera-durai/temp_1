######################################### CREATE GLUE JOB ######################################################


# import boto3

# # Initialize a boto3 client for Glue
# glue_client = boto3.client('glue')

# # Define the parameters for the Glue job
# response = glue_client.create_job(
#     Name='newgluejob',  # Specify a unique job name
#     Role='arn:aws:iam::493316969816:role/Belight-Glue',  # Replace with your IAM role ARN
#     Command={
#         'Name': 'glueetl',  # 'glueetl' for Spark jobs
#         'ScriptLocation': 's3://aws-glue-assets-493316969816-ap-south-1/scripts/1_LOS3.0_customerCreditDocCustom.py',  # Path to the Python script in S3
#         'PythonVersion': '3'  # Use Python 3
#     },
#     DefaultArguments={
#         '--job-language': 'python',
#         '--enable-continuous-cloudwatch-log': 'true',
#         '--enable-spark-ui': 'true',  # Enables the Spark UI
#         '--enable-glue-datacatalog': 'true',  # Enables Data Catalog integration
#         '--TempDir': 's3://aws-glue-assets-493316969816-ap-south-1/temporary/'  # Temporary directory for Glue jobs
#     },
#     MaxRetries=0,  # Number of retries if the job fails
#     Timeout=2880,  # Timeout in minutes
#     NumberOfWorkers=10,  # Number of workers (for Glue 3.0 or higher)
#     WorkerType='G.2X',  # Type of workers (G.1X or G.2X for Glue 3.0 or higher)
#     GlueVersion='4.0',  # Glue version (2.0, 3.0, or 4.0)
# )

# # Print the response
# print(f"Job created: {response['Name']}")








######################################### UPDATE GLUE JOB ######################################################










import boto3

def update_glue_job():
    # Initialize a boto3 client for Glue
    glue_client = boto3.client('glue')

    # Define the updated parameters for the Glue job
    response = glue_client.update_job(
        JobName='newgluejob',  # The name of the Glue job you want to update
        JobUpdate={
            'Role': 'arn:aws:iam::493316969816:role/Belight-Glue',  # Update IAM role if needed
            'Command': {
                'Name': 'glueetl',  # 'glueetl' for Spark jobs
                'ScriptLocation': 's3://aws-glue-assets-493316969816-ap-south-1/scripts/LOS3.0_Insurance__Ins_App_CPDL_KYC_1.py',  # Updated script location
                'PythonVersion': '3'  # Use Python 3
            },
            'DefaultArguments': {
                '--job-language': 'python',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',  # Enables the Spark UI
                '--enable-glue-datacatalog': 'true',  # Enables Data Catalog integration
                '--TempDir': 's3://aws-glue-assets-493316969816-ap-south-1/temporary/'  # Temporary directory for Glue jobs
            },
            'MaxRetries': 1,  # Update the number of retries if needed
            'Timeout': 1440,  # Update the timeout in minutes
            'NumberOfWorkers': 5,  # Update the number of workers
            'WorkerType': 'G.1X',  # Update the worker type
            'GlueVersion': '3.0'  # Update Glue version (2.0, 3.0, or 4.0)
        }
    )

    return response

# Call the function to update the job
response = update_glue_job()
print(f"Job updated: {response['JobName']}")
