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
# Replace None (NaN) values with the string "NULL"
bvd_transaction = bvd_transaction.toPandas().replace({pd.NA: "NULL", None: "NULL"})
bvd_transaction.to_csv("s3://datawarehouse-bilight/Staging/LOS3.0/dmDetails/Yearly/testing/bvd_transaction.txt",index=False, encoding="utf-8-sig")

bvd_transaction.show()
bvd_transaction.printSchema()
