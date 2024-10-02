import os
import configparser


def load_aws_config(config_file='aws_localstack_config.ini', profile='default'):
    config = configparser.ConfigParser()
    # Debugging line
    print(f"Loading config file from: {os.path.abspath(config_file)}")

    if not os.path.exists(config_file):
        raise FileNotFoundError(f"The config file '{config_file}' does not exist.")
    
    config.read(config_file)
    
    if profile not in config:
        raise KeyError(f"The profile '{profile}' does not exist in the configuration file.")
    
    # Set environment variables for AWS
    os.environ['AWS_ACCESS_KEY_ID'] = config[profile]['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config[profile]['aws_secret_access_key']
    os.environ['AWS_DEFAULT_REGION'] = config[profile]['region']
