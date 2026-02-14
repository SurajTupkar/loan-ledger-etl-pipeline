from utils.spark_session import get_spark_session, load_config

from extract.s3_reader import read_raw_loan_data
from quality.data_quality import validate_loan_data
from transform.data_cleaning import clean_loan_data
from transform.business_rules import apply_business_rules

from load.s3_writer import write_to_s3
# from load.rds_writer import write_to_rds

from utils.logger import get_logger

from pyspark.sql.functions import col


logger = get_logger("LoanLedger-ETL")


def run_etl():
    logger.info("ETL Job Started")
    # -------------------------------
    # Spark & Config
    # -------------------------------

    spark = get_spark_session()
    logger.info("Spark session created")
    config = load_config()

    env = config["app"]["env"]

    input_path = config["paths"][env]["raw_path"]
    curated_path = config["paths"][env]["curated_path"]
    rejected_path = config["paths"][env]["rejected_path"]

    logger.info(f"Environment: {env}")
    logger.info(f"Reading data from: {input_path}")

    rds_url = config["rds"]["url"]
    rds_table = config["rds"]["table"]
    rds_user = config["rds"]["user"]
    rds_password = config["rds"]["password"]


    # Extract
    raw_df = read_raw_loan_data(spark, input_path)
    logger.info(f"Raw records count: {raw_df.count()}")
    

    # Data Quality
    valid_df, rejected_df = validate_loan_data(raw_df)
    logger.info(f"Valid records: {valid_df.count()}")
    logger.info(f"Rejected records: {rejected_df.count()}")

    # Transform
    cleaned_df = clean_loan_data(valid_df)
    final_df = apply_business_rules(cleaned_df)


    # Load – Rejected to S3
    logger.info("Writing rejected records to S3/local")
    write_to_s3(
        df=rejected_df,
        output_path=rejected_path,
        file_format="csv",
        mode="overwrite"
    )

    # Load – Curated to S3
    logger.info("Writing rejected records to S3/local")
    write_to_s3(
        df=final_df,
        output_path=curated_path,
        file_format="parquet",
        mode="overwrite"
    )


    # Load – RDS
    logger.info("Writing final data to RDS")
    # write_to_rds(
    #     df=final_df,
    #     jdbc_url=rds_url,
    #     table=rds_table,
    #     user=rds_user,
    #     password=rds_password,
    #     batch_size=1000,
    #     num_partitions=4
    # )
    logger.info("ETL Job Completed Successfully")


if __name__ == "__main__":
    run_etl()
