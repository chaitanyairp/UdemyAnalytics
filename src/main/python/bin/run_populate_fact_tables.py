from pyspark.sql import functions as f

import variables as gav


def populate_fact_tables(spark, user_event_df):

    campaign_dim_df = spark.read\
        .format("parquet")\
        .option("path", gav.campaign_dim_file_path)\
        .load()

    campaign_dim_df = campaign_dim_df.select(
        f.col("campaign_key"),
        f.col("campaign_id").alias("campaign_dim_id"),
        f.col("course_campaign_name").alias("campaign_dim_name"),
        "campaign_start_date",
        "campaign_end_date"
    )

    user_campaign_join_cond = ((f.col("campaign_id") == f.col("campaign_dim_id"))
                               & (f.col("course_campaign_name") == f.col("campaign_dim_name")))
    user_event_df = user_event_df.join(campaign_dim_df, user_campaign_join_cond).drop(
        "campaign_dim_id",
        "campaign_dim_name"
    )
    user_event_df.show(truncate=False)