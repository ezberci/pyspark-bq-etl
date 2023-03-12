from pyspark.sql import SparkSession
from google.cloud import storage
import logging
import sys
import json

logging.basicConfig(level=logging.INFO)

config_name = sys.argv[1]
bucket_name = "us-central1-spark-etl-d30c21af-bucket"
blob_name = "dags/configs/"+config_name

logging.info("config_name: %s", config_name)
logging.info("bucket_name: %s", bucket_name)
logging.info("blob_name: %s", blob_name)


storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
content = blob.download_as_string()
config = json.loads(content)


extract_query = config['extract_query']
transform_query = config['transform_query']
load_table = config['load_table']

logging.info("extract_query: %s", extract_query)
logging.info("transform_query: %s", transform_query)
logging.info("load_table: %s", load_table)



spark = SparkSession \
    .builder \
    .getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://78.184.25.176:55000/") \
    .option("query", extract_query) \
    .option("user", "postgres") \
    .option("password", "123456") \
    .load()

jdbcDF.createOrReplaceTempView("extract_table")

filteredDF = spark.sql(transform_query)

filteredDF.write.format("bigquery") \
    .option("credentialsFile", "/usr/local/spark/credentials/credentialsFile.json") \
    .option("writeMethod", "direct") \
    .save(load_table)
