from pyspark.sql import DataFrame

def write_to_s3(
        df:DataFrame,
        output_path:str,
        file_format:str="csv",
        mode:str="overwrite",
        partition_cols:list=None
):
    
    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if file_format =="csv":
        (
            writer
            .option("header","true")
            .option("emptyValue","")
            .csv(output_path)
        )
    elif file_format == "parquet":
        writer.parquet(output_path)

    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
         