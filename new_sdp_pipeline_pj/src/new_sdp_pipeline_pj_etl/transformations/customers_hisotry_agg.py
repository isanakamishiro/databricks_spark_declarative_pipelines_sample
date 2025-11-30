from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="customers_history_agg",
    comment="Aggregated customer history",
)
def customers_history_agg():
    return (
        spark.read.table("customers_history")
        .groupBy("id")
        .agg(
            F.count("address").alias("address_count"),
            F.count("email").alias("email_count"),
            F.count("firstname").alias("firstname_count"),
            F.count("lastname").alias("lastname_count"),
        )
    )
