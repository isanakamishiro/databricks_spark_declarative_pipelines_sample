from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DateType
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dim_calendar",
    comment="カレンダー軸",
    schema="""
        date_id DATE NOT NULL PRIMARY KEY
        """,
)
def dim_calendar():
    df = spark.read.table("qiita_items")

    # created_at、updated_at、imported_atの最小値、最大値を取得し、その範囲でカレンダーデータを作成
    df = (
        df.groupBy()
        .agg(
            F.min(F.least("created_at", "updated_at", "imported_at")).alias(
                "start_date"
            ),
            F.max(F.greatest("created_at", "updated_at", "imported_at")).alias(
                "end_date"
            ),
        )
        .withColumn(
            "date_seq",
            F.sequence(
                F.to_date("start_date"),
                F.to_date("end_date"),
                F.expr("INTERVAL 1 DAY"),
            ),
        )
        .select(F.explode("date_seq").alias("date_id"))
    )
    return df

@dp.materialized_view(
    name="dim_author",
    comment="投稿者軸",
    schema="""
        author_id STRING NOT NULL PRIMARY KEY,
        author_name STRING
        """,
)
def dim_author():
    df = spark.read.table("qiita_items")

    # 記事データ履歴から生成するため、執筆者名称が変わっている可能性がある。
    # そのため、新しい投稿記事から執筆者名称を取得する
    window = Window.partitionBy("author_id").orderBy(F.col("updated_at").desc())
    df = df.withColumn("rank", F.row_number().over(window))
    df = df.filter(F.col("rank") == 1).select("author_id", "author_name")

    # author_nameが設定されていない場合、idと同一にする
    df = df.withColumn(
        "author_name",
        F.when(
            F.col("author_name").isNull()
            | (F.length(F.trim(F.col("author_name"))) == 0),
            F.col("author_id"),
        ).otherwise(F.col("author_name")),
    )

    return df


@dp.materialized_view(
    name="dim_item_category",
    comment="記事カテゴリ軸",
    schema="""
        item_category_id STRING NOT NULL PRIMARY KEY,
        item_category_name STRING
        """,
)
def dim_item_category():
    df = spark.read.table("qiita_items")

    df = (
        df.select(F.col("auto_item_category").alias("item_category_name"))
        .distinct()
        .filter(F.col("item_category_name").isNotNull())
    )
    # 手抜きでカテゴリ名そのままをidとして扱う
    df = df.withColumn("item_category_id", F.col("item_category_name"))

    return df
