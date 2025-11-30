from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="sumarized_items",
    comment="品質管理とクリーンナップされたQiita投稿データ",
    table_properties={
        "delta.feature.variantType-preview": "supported",
        "delta.enableChangeDataFeed": "true",
    },
)
def ai_transform():
    df = spark.readStream.table("qiita_items")

    # 要約作成
    summary_expr = "ai_summarize(body, 400)"
    df = df.withColumn("summarized_body", F.expr(summary_expr))

    # 要約英訳
    translate_expr = "ai_translate(summarized_body, 'en')"
    df = df.withColumn("en_summarized_body", F.expr(translate_expr))

    return df
