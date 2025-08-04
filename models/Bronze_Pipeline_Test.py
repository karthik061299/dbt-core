from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import time
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("BronzeLayerIngestion").getOrCreate()

# Securely load credentials
source_db_url = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KConnectionString")
user = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KUser")
password = mssparkutils.credentials.getSecret("https://akv-poc-fabric.vault.azure.net/", "KPassword")

# Get current user's identity
try:
    current_user = mssparkutils.env.getUserName()
except:
    try:
        current_user = spark.sparkContext.sparkUser()
    except:
        current_user = "Unknown User"

# Define source and target details
source_system = "PostgreSQL"
database_name = "DE"
schema_name = "tests"
target_bronze_path = "abfss://Analytical_POC@onelake.dfs.fabric.microsoft.com/Inventory.Lakehouse/Tables/Bronze/"

# Define tables to ingest and their source mapping
bronze_table_mappings = [
    ("Applicants", "bz_applicants"),
    ("Applications", "bz_applications"),
    ("Card_Products", "bz_card_products"),
    ("Credit_Scores", "bz_credit_scores"),
    ("Document_Submissions", "bz_document_submissions"),
    ("Verification_Results", "bz_verification_results"),
    ("Underwriting_Decisions", "bz_underwriting_decisions"),
    ("Campaigns", "bz_campaigns"),
    ("Application_Campaigns", "bz_application_campaigns"),
    ("Activations", "bz_activations"),
    ("Fraud_Checks", "bz_fraud_checks"),
    ("Offers", "bz_offers"),
    ("Offer_Performance", "bz_offer_performance"),
    ("Address_History", "bz_address_history"),
    ("Employment_Info", "bz_employment_info")
]

# Define audit table schema
bronze_audit_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
    StructField("processed_by", StringType(), False),
    StructField("processing_time", DoubleType(), False),
    StructField("status", StringType(), False)
])

def log_audit(record_id, source_table, processing_time, status):
    current_time = datetime.now()
    audit_df = spark.createDataFrame([
        (
            record_id,
            source_table,
            current_time,
            current_user,
            processing_time,
            status
        )
    ], schema=bronze_audit_schema)
    audit_df.write.format("delta").mode("append").saveAsTable("Bronze.bz_audit")

def load_to_bronze(source_table, bronze_table, record_id):
    start_time = time.time()
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", source_db_url) \
            .option("dbtable", f'{schema_name}."{source_table}"') \
            .option("user", user) \
            .option("password", password) \
            .load()
        row_count = df.count()
        df = df.withColumn("load_timestamp", current_timestamp()) \
               .withColumn("update_timestamp", current_timestamp()) \
               .withColumn("source_system", lit(source_system))
        df.write.format("delta") \
            .mode("overwrite") \
            .save(f"{target_bronze_path}{bronze_table}")
        processing_time = float(time.time() - start_time)
        log_audit(
            record_id=record_id,
            source_table=source_table,
            processing_time=processing_time,
            status=f"Success - {row_count} rows processed"
        )
        print(f"Successfully loaded {bronze_table} with {row_count} rows")
    except Exception as e:
        processing_time = float(time.time() - start_time)
        log_audit(
            record_id=record_id,
            source_table=source_table,
            processing_time=processing_time,
            status=f"Failed - {str(e)}"
        )
        print(f"Failed to load {bronze_table}: {str(e)}")
        raise e

try:
    print(f"Starting data ingestion process by user: {current_user}")
    for idx, (src_tbl, bronze_tbl) in enumerate(bronze_table_mappings, 1):
        load_to_bronze(src_tbl, bronze_tbl, str(idx))
except Exception as e:
    print(f"Ingestion process failed: {str(e)}")
finally:
    spark.stop()
