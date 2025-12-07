from pyspark import pipelines as dp
import pyspark.sql.functions as F

qiita_catalog = spark.conf.get("src_catalog")
qiita_schema = spark.conf.get("src_schema")
qiita_items = spark.conf.get("src_items")
qiita_items_backfill = spark.conf.get("src_items_backfill")

# CDCデータを格納するためのStreaming Tableを作成
dp.create_streaming_table(
    "qiita_cdc_items_raw",
    comment="新しいQiitaの投稿データを追記する",
)


# Qiitaの記事データをStreaming Tableに追記するフロー
@dp.append_flow(
    target="qiita_cdc_items_raw",
    name="qiita_cdc_items_raw_ingest_flow",
)
def qiita_cdc_items_raw_ingest_flow():
    return spark.readStream.table(f"`{qiita_catalog}`.`{qiita_schema}`.`{qiita_items}`")


# Qiitaのバックフィル用記事データをStreaming Tableに追記するフロー。一度だけしか実行されない。
@dp.append_flow(
    target="qiita_cdc_items_raw",
    name="qiita_cdc_items_backfill_raw_ingest_flow",
    once=True,
)
def qiita_cdc_items_backfill_raw_ingest_flow():
    return spark.read.table(
        f"`{qiita_catalog}`.`{qiita_schema}`.`{qiita_items_backfill}`"
    )


# QiitaのCDC記事データを成形したStreaming Tableを作成。Excepectation付。
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

    # variantデータから一部の情報を抽出した列を作成
    df = df.withColumn("author_id", F.try_variant_get("user", "$.id", "string"))
    df = df.withColumn("author_name", F.try_variant_get("user", "$.name", "string"))
    df = df.withColumn(
        "tag_names",
        F.expr("transform(tags::array<variant>, x -> replace(x:name, '\"', ''))"),
    )

    return df
