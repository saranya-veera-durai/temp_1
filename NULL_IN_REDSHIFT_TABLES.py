
# import sys
# import json
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from awsglue import DynamicFrame

# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# sc = SparkContext()                                                                                                                         
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# ############################################################################################################################
# ########################################### List of CSV files and their corresponding Redshift table #######################
# ############################################################################################################################

# files_and_tables = [
#     {
#         "s3_path": "s3://datawarehouse-bilight/Staging/LOS1.0/eligibilityDetails/Yearly/TEXTF/ED_IDL_CashProfit.txt",
#         "table_name": "deviationmatrix.testing_table_1"
#     },
#     {
#         "s3_path": "s3://datawarehouse-bilight/Staging/LOS1.0/eligibilityDetails/Yearly/TEXTF/ED_IDL_GrossProfit.txt",
#         "table_name": "deviationmatrix.testing_table_2"
#     }
#     # {
#     #     "s3_path": "s3://datawarehouse-bilight/Staging/LOS1.0/camDetails/Yearly/TEXTF/pba_balancesheet.txt",
#     #     "table_name": "deviationmatrix.pba_balancesheet_New"
#     # },
#     # {
#     #     "s3_path": "s3://datawarehouse-bilight/Staging/LOS1.0/camDetails/Yearly/TEXTF/PLandBS_ProfitAndLoss.txt",
#     #     "table_name": "deviationmatrix.PLandBS_ProfitAndLoss_New"
#     # },
#     # {
#     #     "s3_path": "s3://datawarehouse-bilight/Staging/LOS1.0/camDetails/Yearly/TEXTF/PLandBS_Ratios.txt",
#     #     "table_name": "deviationmatrix.PLandBS_Ratios_New"
#     # },
#     # {
#     #     "s3_path": "s3://datawarehouse-bilight/Staging/LOS1.0/camDetails/Yearly/TEXTF/rtrdetails.txt",
#     #     "table_name": "deviationmatrix.rtrdetails_New"
#     # }
#     # Add more files and tables as needed
# ]
# ############################################################################################################################
# ########################################### Process each file and write to Redshift ########################################
# ############################################################################################################################

# for file_info in files_and_tables:
#     # Read the CSV file into a DynamicFrame
#     dynamic_frame = glueContext.create_dynamic_frame.from_options(
#         format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False, "nullValue": ""},
#         connection_type="s3",
#         format="csv",
#         connection_options={"paths": [file_info["s3_path"]], "recurse": True},
#         transformation_ctx=f"AmazonS3_{file_info['table_name']}"
#     )
    
#     # Transform the DynamicFrame to replace empty strings with None
#     transformed_df = dynamic_frame.toDF().na.replace('', None)  # Replace empty strings with None
#     transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")

#     # Write the DynamicFrame to Redshift
#     glueContext.write_dynamic_frame.from_options(
#         frame=transformed_dynamic_frame,
#         connection_type="redshift",
#         connection_options={
#             "redshiftTmpDir": "s3://aws-glue-assets-493316969816-ap-south-1/temporary/",
#             "useConnectionProperties": "true",
#             "dbtable": file_info["table_name"],
#             "connectionName": "Redshift connection New Latest"
#         },
#         transformation_ctx=f"AmazonRedshift_{file_info['table_name']}"
#     )

# job.commit()
