from pyspark.sql import DataFrame

from utils.logger import get_logger

logger = get_logger("RDS-Writer")




# def write_to_rds(
#         df:DataFrame,
#         jdbc_url:str,
#         table:str,
#         user:str,
#         password:str,
#         mode:str="append",
#         batch_size:int=1000,
#         num_partitions:int =4
# ):
#     logger.info(f"Writing data to RDS table {table}")
#     df.repartition(num_partitions)\
#     .write\
#     .format("jdbc")\
#     .option("url",jdbc_url)\
#     .option("dbtable",table)\
#     .option("user",user)\
#     .option("password",password)\
#     .option("batchsize",batch_size)\
#     .option("driver","com.mysql.cj.jdbc.Driver")\
#     .mode(mode)\
#     .save()
#     logger.info(f"Write data to RDS table successfully : {table}")
    