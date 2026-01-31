from pyspark import pipelines as dp
import pyspark.sql.functions as F

LLM_ENDPOINT = "databricks-qwen3-next-80b-a3b-instruct"


# AI Functionsを使って要約や埋め込みを直接追加
@dp.table(
    name="qiita_cdc_ai_augmented_items",
    comment="LLMによって拡張したQiita投稿データ",
    table_properties={
        "delta.feature.variantType-preview": "supported",
        "delta.enableChangeDataFeed": "true",
    },
)
def add_ai_summary():
    # skipChangeCommitsで変更は無視する。記事の変更を加味したい場合は、処理の場所を変えよう
    df = spark.readStream.table("qiita_cdc_cleaned_items")

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

    # 自動カテゴリ
    responseFormat = """{
        "type": "json_schema",
        "json_schema": {
            "name": "item_category",
            "schema": {
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "enum": ["DataEngineer", "DataScience", "DataGovernance"]
                    },
                    "keywords": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["category", "keywords"],
                "additionalProperties": false
            },
            "strict": true
        }
    }"""

    categorize_expr = f"""
    ai_query('{LLM_ENDPOINT}', CONCAT('次の記事をDataEngineer, DataScience, DataGovernanceのいずれかにカテゴリ分けし、関連するキーワードを抽出してください:', en_summarized_body), responseFormat => '{responseFormat}')
    """
    df = df.withColumn("category_and_keywords", F.expr(categorize_expr))
    df = df.withColumn(
        "category_and_keywords_var", F.try_parse_json("category_and_keywords")
    )
    df = df.withColumn(
        "auto_item_category",
        F.try_variant_get("category_and_keywords_var", "$.category", "string"),
    )
    df = df.withColumn(
        "auto_item_keywords",
        F.try_variant_get("category_and_keywords_var", "$.keywords", "array<string>"),
    )

    return df
