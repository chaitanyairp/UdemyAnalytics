from pyspark.sql import functions as f

import variables as gav


def populate_fact_tables(spark, user_event_df):

    campaign_dim_df = spark.read\
        .format("parquet")\
        .option("path", gav.campaign_dim_file_path)\
        .load()

    campaign_dim_df = campaign_dim_df.select(
        "campaign_key",
        "campaign_id",
        "course_campaign_name",
        "campaign_start_date",
        "campaign_end_date"
    )
    campaign_dim_df = campaign_dim_df.alias("campaign_dim_df")
    user_event_df = user_event_df.alias("user_event_df")

    user_campaign_join_cond = ((f.col("campaign_dim_df.campaign_id") == f.col("user_event_df.campaign_id"))
                               & (f.col("campaign_dim_df.course_campaign_name") == f.col("user_event_df.course_campaign_name")))
    user_event_df = user_event_df.join(campaign_dim_df, user_campaign_join_cond).drop(
        *[campaign_dim_df.campaign_id,campaign_dim_df.course_campaign_name])

    user_event_df.show(truncate=False)