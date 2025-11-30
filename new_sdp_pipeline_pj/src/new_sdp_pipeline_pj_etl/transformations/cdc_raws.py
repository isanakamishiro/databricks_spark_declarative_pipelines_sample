from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Replace with the catalog and schema name that
# you are using:
path = "/Volumes/workspace/default/raw_data/customers"


# Create the target bronze table
dp.create_streaming_table(
    "customers_cdc_bronze",
    comment="New customer data incrementally ingested from cloud object storage landing zone",
)


# Create an Append Flow to ingest the raw data into the bronze table
@dp.append_flow(
    target="customers_cdc_bronze",
    name="customers_bronze_ingest_flow",
)
def customers_bronze_ingest_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{path}")
    )
