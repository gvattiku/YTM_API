from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

input_file = 'input.csv'
output_file = 'output.csv'
temp_delimiter = '###'  # Choose a delimiter not present in the CSV

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the CSV file using spark-csv with custom options
df = spark.read.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("inferSchema", "true") \
    .option("delimiter", temp_delimiter) \
    .load(input_file)

# Remove backslashes and commas within the fields
df = df.select([regexp_replace(c, r"[,\\]", "").alias(c) for c in df.columns])

# Convert the temporary delimiter back to commas
df = df.selectExpr([f'replace(`{c}`, "{temp_delimiter}", ",") as `{c}`' for c in df.columns])

# Write the modified dataframe to an output CSV file
df.write.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("emptyValue", "") \
    .mode("overwrite") \
    .save(output_file)

print("CSV file with replaced backslashes, commas, and properly escaped quotes has been created.")
