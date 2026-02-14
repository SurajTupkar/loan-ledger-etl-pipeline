from pyspark.sql.functions import col, when, lit

def validate_loan_data(df):

    valid_condition = (
        col("sl_no").isNotNull() &
        col("installment").isNotNull() & (col("installment") > 0) &
        col("int_rate").isNotNull() &
        col("fico").isNotNull()
    )

    valid_df = df.filter(valid_condition)

    rejected_df = df.filter(~valid_condition) \
        .withColumn(
            "rejection_reason",
            when(col("sl_no").isNull(), lit("NULL_LOAN_ID"))
            .when(col("installment").isNull(), lit("NULL_LOAN_AMOUNT"))
            .when(col("installment") <= 0, lit("INVALID_LOAN_AMOUNT"))
            .when(col("int_rate").isNull(), lit("NULL_INTEREST_RATE"))
            .when(col("fico").isNull(), lit("NULL_FICO"))
            .otherwise(lit("UNKNOWN_REASON"))
        )

    return valid_df, rejected_df
