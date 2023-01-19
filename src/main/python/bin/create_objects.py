import  variables as gav

from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
import logging.config

logging.config.fileConfig(fname=gav.config_file_path)
logger = logging.getLogger(__name__)


def get_spark_session():
    """
    This method is used to create spark session.
    :return: spark
    """
    try:
        logger.info("Started get_spark_session().")
        spark_conf = SparkConf()
        spark_conf.set("spark.app.name", "Udemy Analytics")
        spark_conf.set("spark.master", "local[*]")

        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    except Exception as exp:
        logger.error("Error in get_spark_session()..", exc_info=True)
        raise
    else:
        logger.info("Completed get_spark_session().\n")
        return spark