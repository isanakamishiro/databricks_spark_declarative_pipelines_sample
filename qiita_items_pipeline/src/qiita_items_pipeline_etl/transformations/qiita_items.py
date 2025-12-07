from pyspark import pipelines as dp
import pyspark.sql.functions as F

# CDCデータから重複のない記事データを格納するためのStreaming Tableを作成
dp.create_streaming_table(
    name="qiita_items",
    comment="Qiitaの投稿データ",
    table_properties={"delta.feature.variantType-preview": "supported"},
)

# Auto CDC flow機能を使って、CDCデータを変換して、Streaming Tableに流す
dp.create_auto_cdc_flow(
    target="qiita_items",
    source="qiita_cdc_items_clean",
    keys=["id"],
    sequence_by=F.col("updated_at"),
    ignore_null_updates=False,
)
