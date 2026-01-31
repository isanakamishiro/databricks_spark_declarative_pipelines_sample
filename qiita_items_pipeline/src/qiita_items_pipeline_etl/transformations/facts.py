from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="fact_item",
    comment="Qiita記事Fact",
    schema="""
        id STRING NOT NULL PRIMARY KEY,
        created_date_id DATE NOT NULL,
        updated_date_id DATE NOT NULL,
        imported_date_id DATE NOT NULL,
        author_id STRING NOT NULL,
        item_category_id STRING,
        tag_ids ARRAY<STRING>,
        item_keywords ARRAY<STRING>,
        likes_count INT,
        comments_count INT,
        reactions_count INT,
        stocks_count INT,
        page_views_count INT,
        CONSTRAINT dim_calendar_created_fk FOREIGN KEY(created_date_id) REFERENCES dim_calendar(date_id),
        CONSTRAINT dim_calendar_updated_fk FOREIGN KEY(updated_date_id) REFERENCES dim_calendar(date_id),
        CONSTRAINT dim_calendar_imported_fk FOREIGN KEY(imported_date_id) REFERENCES dim_calendar(date_id),
        CONSTRAINT dim_author_fk FOREIGN KEY(author_id) REFERENCES dim_author,
        CONSTRAINT dim_item_category_fk FOREIGN KEY(item_category_id) REFERENCES dim_item_category
        """,
)
def fact_item():
    df = spark.read.table("qiita_items")

    df = df.select(
        F.col("id"),
        F.to_date(F.col("created_at")).alias("created_date_id"),
        F.to_date(F.col("updated_at")).alias("updated_date_id"),
        F.to_date(F.col("imported_at")).alias("imported_date_id"),
        F.col("author_id"),
        F.col("auto_item_category").alias("item_category_id"),
        F.col("tag_names").alias("tag_ids"),
        F.col("auto_item_keywords").alias("item_keywords"),
        F.col("likes_count"),
        F.col("comments_count"),
        F.col("reactions_count"),
        F.col("stocks_count"),
        F.col("page_views_count"),
    )

    return df
