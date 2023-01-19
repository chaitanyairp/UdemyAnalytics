import os

# Set variables
os.environ["env"] = "TEST"

# Send back variables
env = os.environ["env"]

# Path for staging files
staging_dir = os.getcwd() + "\\..\\staging"
campaign_details_file_path = staging_dir + "\\campaign_details\\"
hubspot_email_click_link_path = staging_dir + "\\hubspot_email_click_link_event\\"
hubspot_email_open_path = staging_dir + "\\hubspot_email_open_event\\"
hubspot_email_sent_path = staging_dir + "\\hubspot_email_sent_event\\"
hubspot_email_unsubscribe_path = staging_dir + "\\hubspot_email_unsubscribe_event\\"
user_details_path = staging_dir + "\\user_details\\"

# Config file name
config_file_path = r"C:\Users\chait\PycharmProjects\Udemy Analytics\src\main\python\config\log_to_file.conf"

# output dimension files
campaign_dim_file_path = staging_dir + "\\dimensions\\campaign\\"
user_dim_path = staging_dir + "\\dimensions\\user\\"
country_dim_path = staging_dir + "\\dimensions\\country\\"
device_dim_path = staging_dir + "\\dimensions\\device\\"
marketing_dim_path = staging_dir + "\\dimensions\\marketing\\"
