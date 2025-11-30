from pyspark import pipelines as dp
import pyspark.sql.functions as F

qiita_catalog = spark.conf.get("qiita_catalog")
qiita_schema = spark.conf.get("qiita_schema")
qiita_items = spark.conf.get("qiita_items")
qiita_items_backfill = spark.conf.get("qiita_items_backfill")

dp.create_streaming_table(
    "qiita_cdc_items_raw",
    comment="新しいQiitaの投稿データを追記する",
)


@dp.append_flow(
    target="qiita_cdc_items_raw",
    name="qiita_cdc_items_raw_ingest_flow",
)
def qiita_cdc_items_raw_ingest_flow():
    return spark.readStream.table(f"`{qiita_catalog}`.`{qiita_schema}`.`{qiita_items}`")


@dp.append_flow(
    target="qiita_cdc_items_raw",
    name="qiita_cdc_items_backfill_raw_ingest_flow",
    once=True,
)
def qiita_cdc_items_backfill_raw_ingest_flow():
    return spark.read.table(
        f"`{qiita_catalog}`.`{qiita_schema}`.`{qiita_items_backfill}`"
    )


@dp.table(
    name="qiita_cdc_items_clean",
    comment="品質管理とクリーンナップされたQiita投稿データ",
    table_properties={"delta.feature.variantType-preview": "supported"},
)
@dp.expect_all_or_drop(
    {
        "valid_id": "id IS NOT NULL",
    }
)
def qiita_cdc_items_clean():
    df = spark.readStream.table("qiita_cdc_items_raw")

    df = df.withColumn(
        "created_at", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ssXXX")
    )
    df = df.withColumn(
        "updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ssXXX")
    )
    df = df.withColumn("user", F.try_parse_json("user"))
    df = df.withColumn("tags", F.try_parse_json("tags"))

    return df
