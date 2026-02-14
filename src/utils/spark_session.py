import yaml
import os
from pyspark.sql import SparkSession


def load_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "app_config.yaml")

    with open(config_path, "r") as file:
        return yaml.safe_load(file)
    
def get_spark_session():
    config = load_config()
    app_name = config["app"]["name"]

    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )

    return spark