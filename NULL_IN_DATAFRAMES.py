import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
spark = SparkSession.builder.appName("JSONSchema").getOrCreate()
json_file_path = "bankVerificationDetailsCustom .json"
df = spark.read.json(json_file_path)

bvd_transaction = df.select("_id", explode("bankingVerificationList").alias("bankingVerificationList"))
bvd_transaction = bvd_transaction.select("*", "bankingVerificationList.applicantId", explode("bankingVerificationList.transaction").alias("transaction"))
bvd_transaction = bvd_transaction.select("*", "transaction.*")
bvd_transaction = bvd_transaction.drop("bankingVerificationList", "transaction", "date")

# Replace empty strings with None (equivalent to null in Spark)
bvd_transaction = bvd_transaction.na.replace('', None)
# Convert to Pandas DataFrame
pdf = bvd_transaction.toPandas()
# Replace None (NaN) values with the string "NULL"
pdf = pdf.replace({pd.NA: "NULL", None: "NULL"})

pdf.to_csv("bvd_transaction.txt", index=False)
bvd_transaction.show()
bvd_transaction.printSchema()
