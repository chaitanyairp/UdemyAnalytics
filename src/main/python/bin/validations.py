import logging
import logging.config
import pandas

import variables as gav

logging.config.fileConfig(fname=gav.config_file_path)
logger = logging.getLogger(__name__)


def validate_spark_object(spark):
    try:
        logger.info("Started validate_spark_object().")
        dt = spark.sql("select current_date()").collect()
        logger.info("Validating spark object by printing current date")
        logger.info("Current date is : " + str(dt))
        logger.info("Completed validate_spark_object().\n")
    except Exception as exp:
        logger.error("Error in validating spark object()..", exc_info=True)
        raise


def print_count_of_df(df, df_name):
    try:
        logger.info(f"Started print_count_of_df for {df_name}")
        cnt = df.count()
        logger.info(f"Count of {df_name} is : {cnt}")
    except Exception as exp:
        logger.error(f"Error in print_count_of_df for {df_name}", exc_info=True)
        raise
    else:
        logger.info(f"Completed print_count_of_df for {df_name}")


def print_schema_of_df(df, df_name):
    try:
        logger.info(f"Started print_schema_of_df for {df_name}")
        for c in df.schema:
            logger.info(f"\t{c}")

    except Exception as exp:
        logger.error(f"Error in print_count_of_df for {df_name}", exc_info=True)
        raise
    else:
        logger.info(f"Completed print_schema_of_df for {df_name}")


def print_top_ten_rows(df, df_name):
    try:
        logger.info(f"Started print_top_ten_rows for {df_name}")
        df_top_ten = df.limit(10)
        df_pandas = df_top_ten.toPandas()
        logger.info("\n \t" + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error(f"Error in print_top_ten_rows for {df_name}", exc_info=True)
        raise
    else:
        logger.info(f"Completed print_top_ten_rows for {df_name}")




