from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

import variables as gav
from create_schema import *


def populate_fact_tables(spark, user_event_df):

    user_details_schema = create_user_schema()
    campaign_details_schema = create_campaign_schema()

    user_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(user_details_schema) \
        .option("path", gav.user_details_path) \
        .load()

    campaign_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(campaign_details_schema) \
        .option("path", gav.campaign_details_file_path) \
        .load()

    user_dim_df = spark.read\
        .format("parquet")\
        .option("path", gav.user_dim_path)\
        .load()

    country_dim_df = spark.read\
        .format("parquet")\
        .option("path", gav.country_dim_path)\
        .load()

    device_dim_df = spark.read \
        .format("parquet") \
        .option("path", gav.device_dim_path) \
        .load()

    campaign_dim_df = spark.read\
        .format("parquet")\
        .option("path", gav.campaign_dim_file_path)\
        .load()

    marketing_dim_df = spark.read\
        .format("parquet")\
        .option("path", gav.marketing_dim_path)\
        .load()

    user_df = user_df.alias("user_df")
    user_dim_df = user_dim_df.alias("user_dim_df")
    country_dim_df = country_dim_df.alias("country_dim_df")
    device_dim_df = device_dim_df.alias("device_dim_df")

    join_type = "inner"
    user_dim_cond = (f.col("user_df.user_id") == f.col("user_dim_df.user_id"))
    country_dim_cond = ((f.col("user_df.user_country") == f.col("country_dim_df.user_country")) &
                        (f.col("user_df.user_state") == f.col("country_dim_df.user_state")))
    device_dim_cond = (f.col("user_df.user_device_type") == f.col("device_dim_df.device_type"))

    user_joined_df = user_df.join(user_dim_df, on=user_dim_cond, how=join_type)\
        .join(country_dim_df, on=country_dim_cond, how=join_type)\
        .join(device_dim_df, on=device_dim_cond, how=join_type)

    user_all_dim_df = user_joined_df.select(
        f.col("user_dim_df.user_key").alias("user_key"),
        f.col("country_dim_df.country_key").alias("country_key"),
        f.col("device_dim_df.device_key").alias("device_key"),
        f.col("user_df.user_id").alias("user_id"),
        f.col("user_df.user_name").alias("user_name"),
        f.col("user_df.user_country").alias("user_country"),
        f.col("user_df.user_state").alias("user_state"),
        f.col("user_device_type").alias("user_device_type")
    )

    user_all_dim_df.show(truncate=False)

    campaign_dim_df = campaign_dim_df.select("campaign_id", "campaign_key")
    marketing_dim_df = marketing_dim_df.select(f.lower(f.col("marketing_team")).alias("marketing_team"), "marketing_key")

    mark_cond = (f.col("marketing_team") == f.col("digital_marketing_team"))

    campaign_all_df = campaign_df.join(campaign_dim_df, on="campaign_id", how="inner")\
        .join(marketing_dim_df, on=mark_cond, how=join_type)

    campaign_all_df = campaign_all_df.select(
        "campaign_key",
        "marketing_key",
        "campaign_id",
        "course_campaign_name",
        "course_campaign_start_date",
        "course_campaign_end_date",
        "digital_marketing_team"
    )

    campaign_all_df.show(truncate=False)

    user_event_df.createOrReplaceTempView("userEventDetails")

    user_event_df = spark.sql(
         """
            select campaign_id, user_id, event_type, user_response_time_in_days, event_status as event_count
            from userEventDetails  
        """
    )

    user_event_df.show(truncate=False)

    fact_tbl_df = user_event_df.join(user_all_dim_df, on="user_id", how=join_type)\
        .join(campaign_all_df, on="campaign_id", how=join_type)

    fact_tbl = fact_tbl_df.select(
        f.row_number().over(w.orderBy("campaign_id", "user_id")).alias("fact_key"),
        "campaign_key",
        "user_key",
        "marketing_key",
        "country_key",
        "device_key",
        "user_id",
        "user_name",
        "user_country",
        "user_state",
        "user_device_type",
        "campaign_id",
        "course_campaign_name",
        "course_campaign_start_date",
        "course_campaign_end_date",
        "digital_marketing_team",
        "user_response_time_in_days",
        "event_type",
        "event_count",
        f.year("course_campaign_start_date").alias("campaign_year"),
        f.month("course_campaign_start_date").alias("campaign_month"),
        f.dayofmonth("course_campaign_start_date").alias("campaign_date")
    )

    fact_tbl.write.format("csv").option("path", gav.fact_path).save()