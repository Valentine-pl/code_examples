import sys
import boto3
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.sql.functions import col, coalesce, year, month, dayofmonth, to_date, lit

# Initialize AWS Glue context and job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'S3_KEY'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def download_s3_json(bucket, key):
    """Download a JSON file from S3."""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    json_content = json.loads(obj['Body'].read().decode('utf-8'))
    return json_content

def process_table(glueContext, spark, table_config):
    table_name = table_config["table_name"]
    composite_key_cols = table_config["composite_key_cols"]
    connection_name = table_config["connection_name"]
    delta_table_path = table_config["delta_table_path"]
    updated_at_col = table_config["updated_at_col"]
    created_at_col = table_config["created_at_col"]
    load_from_date = table_config.get("load_from_date")

    OracleSQL_node = glueContext.create_dynamic_frame.from_options(
        connection_type="oracle",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": f"{table_name}",
            "connectionName": connection_name,
        },
        transformation_ctx=f"{table_name}_node",
    )

    df = OracleSQL_node.toDF().distinct()
    df = df.withColumn(updated_at_col, coalesce(col(updated_at_col), col(created_at_col)))

    if load_from_date:
        df = df.filter(to_date(col(updated_at_col)) >= to_date(lit(load_from_date)))

    df = df.withColumn("year", year(col(updated_at_col)))\
           .withColumn("month", month(col(updated_at_col)))\
           .withColumn("day", dayofmonth(col(updated_at_col)))\
           .repartition(1)

    if DeltaTable.isDeltaTable(spark, delta_table_path):
        deltaTable = DeltaTable.forPath(spark, delta_table_path)
        deltaTable.alias("existing").merge(
            df.alias("new_data"),
            " AND ".join([f"existing.{col} = new_data.{col}" for col in composite_key_cols])
        ).whenMatchedUpdate(
            condition = col(f"existing.{updated_at_col}") < col(f"new_data.{updated_at_col}"),
            set = {col_name: col("new_data." + col_name) for col_name in df.columns if col_name not in composite_key_cols}
        ).whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").partitionBy("year", "month").option("path", delta_table_path).save()

# Download the configuration from S3 for Tables Config
bucket_name = args['S3_BUCKET']
s3_key = args['S3_KEY']
tables_config = download_s3_json(bucket_name, s3_key)

# Process each table according to the configuration
for table_config in tables_config:
    process_table(glueContext, spark, table_config)

job.commit()
