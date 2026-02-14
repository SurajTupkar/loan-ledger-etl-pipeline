from pyspark.sql.functions import col, when

def clean_loan_data(df):

    return (
        df
        .withColumn(
            "loan_status",
            when(col("not_fully_paid") == 1, "DEFAULTED")
            .otherwise("PAID")
        )
    )
