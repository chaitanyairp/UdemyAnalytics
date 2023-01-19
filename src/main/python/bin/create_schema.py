from pyspark.sql.types import *


def create_campaign_schema():
    campaign_schema = StructType([
        StructField("campaign_id", StringType()),
        StructField("course_campaign_name", StringType()),
        StructField("campaign_agenda", StringType()),
        StructField("campaign_category", StringType()),
        StructField("campaign_agenda_sent", StringType()),
        StructField("campaign_agenda_open", StringType()),
        StructField("campaign_agenda_click", StringType()),
        StructField("campaign_agenda_unsubscribe", StringType()),
        StructField("digital_marketing_team", StringType()),
        StructField("course_campaign_start_date", StringType()),
        StructField("course_campaign_end_date", StringType()),
        StructField("marketing_product", StringType())
    ])
    return campaign_schema


def create_user_schema():
    user_details_schema = StructType([
        StructField("user_id", StringType()),
        StructField("user_name", StringType()),
        StructField("user_email", StringType()),
        StructField("user_country", StringType()),
        StructField("user_state", StringType()),
        StructField("user_timezone", StringType()),
        StructField("user_last_activity", IntegerType()),
        StructField("user_device_type", StringType()),

    ])
    return user_details_schema


def create_event_schema():
    event_details_schema = StructType([
        StructField("campaign_id", StringType()),
        StructField("course_campaign_name", StringType()),
        StructField("user_id", StringType()),
        StructField("user_name", StringType()),
        StructField("campaign_date", StringType()),
        StructField("digital_marketing_team", StringType()),
        StructField("event_status", StringType()),
        StructField("event_type", StringType()),
        StructField("marketing_product", StringType()),
        StructField("user_response_time", StringType())
    ])

    return event_details_schema
