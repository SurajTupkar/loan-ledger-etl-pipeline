from pyspark.sql.functions import col

def apply_business_rules(df):

    return (
        df
        .withColumn(
            "risk_category",
            col("fico").cast("int")
        )
    )
