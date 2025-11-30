from pyspark import pipelines as dp
import pyspark.sql.functions as F

# create the table
dp.create_streaming_table(
    name="customers_history",
    comment="Slowly Changing Dimension Type 2 for customers",
)

# store all changes as SCD2
dp.create_auto_cdc_flow(
    target="customers_history",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=F.col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="2",
)  # Enable SCD2 and store individual updates
