from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.logger import get_logger
logger = get_logger("S3-Reader")


def read_raw_loan_data(spark,input_path):
    schema = StructType([
        StructField("loan_id",IntegerType(),True),
        StructField("customer_id",IntegerType(),True),
        StructField("loan_amount",StringType(),True),
        StructField("interest_rate",DoubleType(),True),
        StructField("loan_status",StringType(),True),
        StructField("loan_start_date",StringType(),True),
        StructField("region",StringType(),True),
        StructField("__corrupt_record",StringType(),True)
    ])
    logger.info(f"started Reading CSV from : {input_path}")
    df = spark.read\
              .format("csv")\
              .option("header","true")\
              .option("mode","PERMISSIVE")\
              .option("columnNameOfCorruptRecord","__corrupt_record")\
              .option("inferSchema","true")\
              .csv(input_path)
            # .schema(schema)\
    
    df.printSchema()
    df.show(5)
    
    logger.info(f"Read data successfully from : {input_path}")
    return df


