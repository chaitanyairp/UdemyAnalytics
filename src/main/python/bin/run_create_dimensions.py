import logging
import logging.config
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

import variables as gav

logging.config.fileConfig(fname=gav.config_file_path)
logger = logging.getLogger(__name__)


def create_user_dim(user_df):
    try:
        logger.info("Started create_user_dim().")

        user_sel_df = user_df.select(
            "user_id",
            "user_name",
            "user_email"
        )

        # There will be no duplicate columns. No need to distinct.
        user_dim_df = user_sel_df.withColumn("user_key", f.concat_ws("_", f.col("user_id"), f.col("user_name"))) \
            .select(
            "user_key",
            "user_id",
            "user_name",
            "user_email"
        )

    except Exception as exp:
        logger.error("Error in creating user_dim().", exc_info=True)
        raise
    else:
        logger.info("Completed create_user_dim().\n")
        return user_dim_df


def create_country_dim(user_df):
    try:
        logger.info("Started create_country_dim()")

        country_df = user_df.select(
            "user_country",
            "user_state",
            "user_timezone"
        )

        # Remove duplicates and keep only distinct values
        country_df = country_df.distinct()
        # Add a country surrogate key
        country_dim_df = country_df.withColumn("country_key",
                                               f.concat_ws("_", f.col("user_country"), f.col("user_state"))) \
            .select(
            "country_key",
            "user_country",
            "user_state",
            "user_timezone"
        )

    except Exception as exp:
        logger.error("Error in creating user_dim().", exc_info=True)
        raise
    else:
        logger.info("Completed create_country_dim().\n")
        return country_dim_df


def create_device_dimension(user_df):
    try:
        logger.info("Started create_device_dimension().")
        device_df = user_df.select(
            f.col("user_device_type").alias("device_type")
        )

        device_df = device_df.distinct()
        device_dim_df = device_df.withColumn("device_key", f.expr("uuid()")).select(
            "device_key",
            "device_type"
        )

    except Exception as exp:
        logger.error("Error in create_device_dimension().", exc_info=True)
        raise
    else:
        logger.info("Completed create_device_dimension().\n", exc_info=True)
        return device_dim_df


def create_campaign_dimension(campaign_df):
    try:
        logger.info("Started create_campaign_dimension().")
        campaign_dim_df = campaign_df.select(
            "campaign_key",
            "campaign_id",
            "course_campaign_name",
            "campaign_start_date",
            "campaign_end_date",
            "campaign_agenda",
            "campaign_category",
            "campaign_agenda_sent",
            "campaign_agenda_open",
            "campaign_agenda_click",
            "campaign_agenda_unsubscribe"
        )
    except Exception as exp:
        logger.error("Error in create_campaign_dimension().", exc_info=True)
        raise
    else:
        logger.info("Completed create_campaign_dimension().\n")
        return campaign_dim_df


def create_marketing_dimension(campaign_df):
    try:
        logger.info("Started create_marketing_dimension().")

        marketing_dim_df = campaign_df.select(
            f.upper(f.col("digital_marketing_team")).alias("marketing_team")
        )

        marketing_dim_df = marketing_dim_df.distinct()

        marketing_dim_df = marketing_dim_df.withColumn("marketing_place",
                                                       f.split(f.col("marketing_team"), "_").getItem(1))
        marketing_dim_df = marketing_dim_df.withColumn("marketing_key",
                                                       f.row_number().over(w.orderBy("marketing_team")))
        marketing_dim_df = marketing_dim_df.select(
            "marketing_key",
            "marketing_team",
            "marketing_place"
        ).repartition(2)

    except Exception as exp:
        logger.error("Error in create_marketing_dimension.", exc_info=True)
        raise
    else:
        logger.info("Completed create_marketing_dimension().\n")
        return marketing_dim_df
