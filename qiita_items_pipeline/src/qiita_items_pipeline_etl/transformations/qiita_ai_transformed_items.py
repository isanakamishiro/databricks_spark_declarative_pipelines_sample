from pyspark import pipelines as dp
import pyspark.sql.functions as F

# AI Functionsを使って要約や埋め込みを直接追加
@dp.table(
    name="qiita_ai_augmented_items",
    comment="品質管理とクリーンナップされたQiita投稿データ",
    table_properties={
        "delta.feature.variantType-preview": "supported",
        "delta.enableChangeDataFeed": "true",
    },
)
def add_ai_summary():
    df = spark.readStream.table("qiita_items")

    # 要約作成
    # bodyの先頭8000文字を取得
    df = df.withColumn("body_head_8000", F.expr("substring(body, 1, 8000)"))
    summary_expr = "ai_summarize(body_head_8000, 400)"
    df = df.withColumn(
        "summarized_body",
        F.concat_ws(
            "\n",
            F.lit("Title:"),
            F.col("title"),
            F.lit("Summary:"),
            F.expr(summary_expr),
        ),
    )

    # 要約英訳
    translate_expr = "ai_translate(summarized_body, 'en')"
    df = df.withColumn("en_summarized_body", F.expr(translate_expr))

    return df
