from services.spark_service import spark_session
from pyspark.sql.functions import col
import os


def start():
    # Get the DF for the CSV file
    data = './data/reviews-250.csv'

    df = spark_session.read.csv("./data/reviews-250.csv", header=True, inferSchema=True)

    # Show top 5
    df.show(5)

    # Filter reviews with a Score of 5
    high_rating_df = df.filter(df["Score"] == 5)
    high_rating_df.show()

    # Select specific columns
    selected_columns_df = df.select("ProductId", "Score", "Summary")
    selected_columns_df.show(5)
    print(selected_columns_df.collect()[0].asDict())

    # Add HelpfulnessRatio column
    df = df.withColumn(
        "HelpfulnessRatio",
        (col("HelpfulnessNumerator") / col("HelpfulnessDenominator")).cast("double")
    )
    df.show(5)




if __name__ == '__main__':
    start()