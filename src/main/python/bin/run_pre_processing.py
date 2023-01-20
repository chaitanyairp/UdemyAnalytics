import logging
import logging.config
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w
from pyspark.sql.types import *

import variables as gav

logging.config.fileConfig(fname=gav.config_file_path)
logger = logging.getLogger(__name__)


def run_user_details_pre_processing(user_df, df_name):
    # select req cols, nulls, duplicates, column_formatting.
    try:
        logger.info("Started run_user_details_pre_processing().\n")

        # Check for duplicates and drop them.
        user_df = user_df.withColumn("rn", f.row_number().over(
            w.partitionBy("user_id").orderBy(f.col("user_last_activity").desc())))
        user_df = user_df.filter("rn = 1").drop("rn", "user_last_activity")

        # # Check for nulls and drop them
        # user_df.select(
        #     [f.count(f.when(f.col(c).isNull() | f.isnan(c), c)).alias(c) for c in user_df.columns]
        # ).show()
        user_df = user_df.dropna(subset=["user_id", "user_device_type", "user_state"])

    except Exception as exp:
        logger.error("Error in user_Details_pre_processing().", exc_info=True)
        raise
    else:
        logger.info("Completed run_user_details_pre_processing().\n")
        return user_df


def run_campaign_pre_processing(campaign_df, df_name):
    try:
        logger.info("Started run_campaign_pre_processing.")
        # Format fields
        campaign_df = campaign_df.select(
            f.concat_ws("_", f.col("campaign_id"), f.col("course_campaign_name")).alias("campaign_key"),
            f.col("campaign_id"),
            "course_campaign_name",
            "campaign_agenda",
            "campaign_category",
            f.col("campaign_agenda_sent").cast("int"),
            f.col("campaign_agenda_open").cast("int"),
            f.col("campaign_agenda_click").cast("int"),
            f.col("campaign_agenda_unsubscribe").cast("int"),
            "digital_marketing_team",
            f.to_date("course_campaign_start_date", "MM/dd/yyyy").alias("campaign_start_date"),
            f.to_date("course_campaign_end_date", "MM/dd/yyyy").alias("campaign_end_date"),
            "marketing_product"
        )

        # Drop duplicates
        campaign_df = campaign_df.withColumn("rn", f.row_number()
            .over(w.partitionBy("campaign_id", "course_campaign_name")
            .orderBy("campaign_start_date"))) \
            .filter("rn = 1") \
            .drop("rn")

        # Drop null values
        campaign_df = campaign_df.dropna(subset=["campaign_id"])

        # Drop rows with incorrect campaign date
        #campaign_df = campaign_df.filter(f.datediff(f.col("campaign_start_date"), f.col("campaign_end_date")) == -14)
    except Exception as exp:
        logger.error("Error in run_campaign_pre_processing().", exc_info=True)
        raise
    else:
        logger.info("Completed run_campaign_pre_processing().\n")
        return campaign_df


def run_user_event_pre_processing(user_event_df, df_name):
    try:
        logger.info(f"Started uer_event_pre_processing() for df {df_name}")
        user_event_df = user_event_df.select(
            f.col("campaign_id").alias("campaign_id"),
            "course_campaign_name",
            f.col("user_id").cast("int"),
            "user_name",
            "campaign_date",
            "digital_marketing_team",
            f.col("event_status").cast("int").alias("event_status"),
            "event_type",
            "marketing_product",
            f.col("user_response_time").cast("int").alias("user_response_time")
        )

        sec_div = 1000 * 60 * 60 * 24

        user_event_df = user_event_df.withColumn("campaign_date", f.to_date("campaign_date", "MM/dd/yyyy"))\
            .withColumn("user_response_time_in_days", f.when(f.col("user_response_time") == -1, f.col("user_response_time"))
                        .otherwise(f.round(f.col("user_response_time")/sec_div, 2)))

    except Exception as exp:
        logger.error(f"Error in pre_processing for df {df_name}", exc_info=True)
        raise
    else:
        logger.info(f"Completed pre_processing for dataframe {df_name}\n")
        return user_event_df


