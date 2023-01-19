import variables as gav


def write_user_dimension(user_dim_df):
    user_dim_df.repartition(4).write\
        .format("parquet")\
        .option("mode", "overwrite")\
        .option("path", gav.user_dim_path)\
        .save()


def write_campaign_dimension(campaign_dim_df):
    campaign_dim_df.repartition(4).write \
        .format("parquet") \
        .option("mode", "overwrite") \
        .option("path", gav.campaign_dim_file_path) \
        .save()


def write_country_dimension(country_dim_df):
    country_dim_df.repartition(4).write \
        .format("parquet") \
        .option("mode", "overwrite") \
        .option("path", gav.country_dim_path) \
        .save()


def write_device_dimension(device_dim_df):
    device_dim_df.repartition(4).write \
        .format("parquet") \
        .option("mode", "overwrite") \
        .option("path", gav.device_dim_path) \
        .save()


def write_marketing_dimension(marketing_dim_df):
    marketing_dim_df.repartition(4).write \
        .format("parquet") \
        .option("mode", "overwrite") \
        .option("path", gav.marketing_dim_path) \
        .save()
