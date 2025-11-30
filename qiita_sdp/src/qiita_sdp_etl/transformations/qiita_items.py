from pyspark import pipelines as dp
import pyspark.sql.functions as F

dp.create_streaming_table(
    name="qiita_items",
    comment="Qiitaの投稿データ",
    table_properties={"delta.feature.variantType-preview": "supported"},
)

dp.create_auto_cdc_flow(
    target="qiita_items",
    source="qiita_cdc_items_clean",
    keys=["id"],
    sequence_by=F.col(
        "updated_at"
    ),
    ignore_null_updates=False,
)
