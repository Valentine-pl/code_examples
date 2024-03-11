import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Initialize AWS Glue context and job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Spark SQL configurations for handling large datasets
spark.conf.set("spark.sql.files.maxRecordsPerFile", "500000")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")

# Custom SQL query
custom_query = "SELECT ta_mispar_pnimi, ta_shem_aroh, ta_taarich_ptiha, ta_taarich_idkun FROM rashutp.taagid"

# Read data using custom SQL query from Oracle SQL database using Spark SQL
df = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//xxx.xx.xx.xx:1521/xxxx") \
    .option("query", custom_query) \
    .option("user", "xxxxxxxxxxx") \
    .option("password", "xxxxxxx") \
    .load()

# Optional: Repartition DataFrame to optimize the number of output files
num_partitions = 1  # Adjust based on your dataset size and desired file size
df = df.repartition(num_partitions)

# Specify the S3 path to your Delta Lake table
delta_table_path = "s3://oracle-glue-folder/data_delta_agg/"

# Check if the Delta table exists and perform upserts or create a new Delta table
if DeltaTable.isDeltaTable(spark, delta_table_path):
    deltaTable = DeltaTable.forPath(spark, delta_table_path)
    # Perform upsert using 'id' as unique identifier column and consider 'updated_at' for updates
    deltaTable.alias("existing").merge(
        df.alias("new_data"),
        "existing.ta_mispar_pnimi = new_data.ta_mispar_pnimi"
    ).whenMatchedUpdate(
        condition="existing.ta_taarich_ptiha < new_data.ta_taarich_ptiha",
        set={col_name: col("new_data." + col_name) for col_name in df.columns if col_name != "ta_mispar_pnimi"}
    ).whenNotMatchedInsertAll().execute()
else:
    # If the Delta table does not exist, create it by writing the DataFrame in Delta format
    df.write.format("delta").option("path", delta_table_path).save()

# Commit the job
job.commit()
