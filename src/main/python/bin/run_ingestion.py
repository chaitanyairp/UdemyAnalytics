import logging
import logging.config
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import variables as gav
from create_schema import create_user_schema, create_campaign_schema, create_event_schema

logging.config.fileConfig(fname=gav.config_file_path)
logger = logging.getLogger(__name__)


def ingest_user_details_file(spark, file_path):
    try:
        f_name = "user_details"
        logger.info(f"Started ingesting the file {f_name}")

        user_details_schema = create_user_schema()

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(user_details_schema) \
            .option("path", file_path) \
            .load()

        logger.info(f"Completed ingesting the file {f_name}")
    except Exception as exp:
        logger.eror(f"Error in ingesting the file at location {f_name}.", exc_info=True)
        raise
    else:
        return df


def ingest_campaign_data(spark, file_path):
    try:
        f_name = "campaign_details.csv"
        logger.info(f"Started ingest_campaign_data() for file {f_name}.")

        campaign_schema = create_campaign_schema()

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(campaign_schema) \
            .option("path", file_path) \
            .load()

    except Exception as exp:
        logger.error("Error in ingest_campaign_data().", exc_info=True)
        raise
    else:
        logger.info("Completed ingest_campaign_data().\n")
        return df


def run_ingest_click_link_data(spark, file_path):
    try:
        logger.info("Started run_ingest_link_data().")
        user_event_schema = create_event_schema()

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(user_event_schema) \
            .option("path", file_path) \
            .load()

    except Exception as exp:
        logger.error("Error in ingest_click_link_data().", exc_info=True)
        raise
    else:
        logger.info("Completed ingest_click_link().\n")
        return df


def run_ingest_open_event_data(spark, file_path):
    try:
        logger.info("Started run_ingest_open_event_data().")
        user_event_schema = create_event_schema()

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(user_event_schema) \
            .option("path", file_path) \
            .load()

    except Exception as exp:
        logger.error("Error in ingest_open_event_data().", exc_info=True)
        raise
    else:
        logger.info("Completed ingest_open_event_data().\n")
        return df


def run_ingest_sent_event_data(spark, file_path):
    try:
        logger.info("Started run_ingest_sent_event_data().")
        user_event_schema = create_event_schema()

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(user_event_schema) \
            .option("path", file_path) \
            .load()

    except Exception as exp:
        logger.error("Error in ingest_sent_event_data().", exc_info=True)
        raise
    else:
        logger.info("Completed ingest_sent_event_data().\n")
        return df


def run_ingest_unsubscribe_event_data(spark, file_path):
    try:
        logger.info("Started run_ingest_unsubscribe_event_data().")
        user_event_schema = create_event_schema()

        df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(user_event_schema) \
            .option("path", file_path) \
            .load()

    except Exception as exp:
        logger.error("Error in ingest_unsubscribe_event_data().", exc_info=True)
        raise
    else:
        logger.info("Completed ingest_unsubscribe_event_data().\n")
        return df

