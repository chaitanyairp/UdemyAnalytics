import sys
import logging
import logging.config

from create_objects import get_spark_session
from validations import validate_spark_object, print_count_of_df, print_schema_of_df, print_top_ten_rows
from run_ingestion import *
from run_pre_processing import *
from run_create_dimensions import create_user_dim, create_country_dim, create_device_dimension, \
    create_campaign_dimension, create_marketing_dimension
from run_write_dimensions import *
from run_populate_fact_tables import *
import variables as gav

logging.config.fileConfig(fname=gav.config_file_path)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Started executing main().")

        # Get spark object
        spark = get_spark_session()

        # Validate spark object
        validate_spark_object(spark)

        # Read user_details_file
        user_details_df = ingest_user_details_file(spark, gav.user_details_path)
        print_count_of_df(user_details_df, "user_details_df")
        print_schema_of_df(user_details_df, "user_details_df")
        print_top_ten_rows(user_details_df, "user_details_df")

        # Clean user_details_df
        user_df = run_user_details_pre_processing(user_details_df, "user_details_df")
        # Cache the dataframe as it is used by multiple dimensions
        user_df = user_df.cache()
        print_schema_of_df(user_df, "user_df")
        print_count_of_df(user_df, "user_df")
        print_top_ten_rows(user_df, "user_df")

        # Generate dimension tables
        # Country dimension -->
        country_dim_df = create_country_dim(user_df)
        print_schema_of_df(country_dim_df, "country_dim_df")
        print_count_of_df(country_dim_df, "country_dim_df")
        print_top_ten_rows(country_dim_df, "country_dim_df")
        write_country_dimension(country_dim_df)

        # User dimension -->
        user_dim_df = create_user_dim(user_df)
        print_schema_of_df(user_dim_df, "user_dim_df")
        print_count_of_df(user_dim_df, "user_dim_df")
        print_top_ten_rows(user_dim_df, "user_dim_df")
        write_user_dimension(user_dim_df)

        # Device dimension -->
        device_dim_df = create_device_dimension(user_df)
        print_schema_of_df(device_dim_df, "device_dim_df")
        print_count_of_df(device_dim_df, "device_dim_df")
        print_top_ten_rows(device_dim_df, "device_dim_df")
        write_device_dimension(device_dim_df)

        user_df.unpersist()

        # Ingest campaign file
        campaign_details_df = ingest_campaign_data(spark, gav.campaign_details_file_path)
        print_count_of_df(campaign_details_df, "campaign_details_df")
        print_schema_of_df(campaign_details_df, "campaign_details_df")
        print_top_ten_rows(campaign_details_df, "campaign_details_df")

        # Clean campaign file
        campaign_df = run_campaign_pre_processing(campaign_details_df, "campaign_details_df")
        # Cache campaign df as it is used in multiple dimensions
        campaign_df = campaign_df.cache()
        print_count_of_df(campaign_df, "campaign_df")
        print_schema_of_df(campaign_df, "campaign_df")
        print_top_ten_rows(campaign_df, "campaign_df")

        # Create Campaign dimension ---->
        campaign_dim_df = create_campaign_dimension(campaign_df)
        print_count_of_df(campaign_dim_df, "campaign_dim_df")
        print_schema_of_df(campaign_dim_df, "campaign_dim_df")
        print_top_ten_rows(campaign_dim_df, "campaign_dim_df")
        write_campaign_dimension(campaign_dim_df)

        # Create Marketing dimension ---->
        marketing_dim_df = create_marketing_dimension(campaign_df)
        print_count_of_df(marketing_dim_df, "marketing_dim_df")
        print_schema_of_df(marketing_dim_df, "marketing_dim_df")
        print_top_ten_rows(marketing_dim_df, "marketing_dim_df")
        write_marketing_dimension(marketing_dim_df)

        campaign_df.unpersist()

        # Ingest user event email click details and clean it
        user_email_click_df = run_ingest_click_link_data(spark, gav.hubspot_email_click_link_path)
        user_email_open_df = run_ingest_open_event_data(spark, gav.hubspot_email_open_path)
        user_email_sent_df = run_ingest_sent_event_data(spark, gav.hubspot_email_sent_path)
        user_email_unsubscribe_df = run_ingest_unsubscribe_event_data(spark, gav.hubspot_email_unsubscribe_path)

        user_email_click_event_df = run_user_event_pre_processing(user_email_click_df, "user_email_click_df")
        user_email_open_df = run_user_event_pre_processing(user_email_open_df, "user_email_open_df")
        user_email_send_df = run_user_event_pre_processing(user_email_sent_df, "user_email_sent_df")
        user_email_unsubscribe_df = run_user_event_pre_processing(user_email_unsubscribe_df, "user_email_unsubscribe_df")

        print_top_ten_rows(user_email_click_event_df, "user_email_click_event_df")
        print_top_ten_rows(user_email_open_df, "user_email_open_df")
        print_top_ten_rows(user_email_send_df, "user_email_send_df")
        print_top_ten_rows(user_email_unsubscribe_df, "user_email_unsubscribe_df")

        user_event_df = user_email_open_df.unionByName(user_email_click_event_df)\
            .unionByName(user_email_send_df)\
            .unionByName(user_email_unsubscribe_df)

        print_top_ten_rows(user_event_df, "user_event_df")
        print_count_of_df(user_event_df, "user_event_df")

        populate_fact_tables(spark, user_event_df)

        logger.info("Completed executing main().")

    except Exception as exp:
        logger.error("Error in main().", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()