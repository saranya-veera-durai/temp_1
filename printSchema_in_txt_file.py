

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Expand Struct Fields") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "16g") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.caseSensitive", True)

# Load the JSON data
json_path = "dmDetails_20241004000000.json"
df = spark.read.json(json_path)

# Capture the schema as a nested tree string and store it in a file
output_file_path = "schema_output.txt"
with open(output_file_path, "w") as file:
    df.printSchema()
    # Capture printSchema output as a string and write it to the file
    file.write(df._jdf.schema().treeString())

# Confirm output
print(f"Nested schema has been written to {output_file_path}")
