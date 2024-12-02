from services.spark_service import spark_session
from pyspark.sql.functions import col, avg
import os


def start():
    # Get the DF for the CSV file
    data = './data/reviews-250.csv'


    # ---- DATAFRAME ----
    df = spark_session.read.csv("./data/reviews-250.csv", header=True, inferSchema=True)
    
    # 1 - Show top 5
    df.show(5)

    # 2 - Filter reviews with a Score of 5
    high_rating_df = df.filter(df["Score"] == 5)
    high_rating_df.show()

    # 3 - Select specific columns
    selected_columns_df = df.select("ProductId", "Score", "Summary")
    selected_columns_df.show(5)
    print(selected_columns_df.collect()[0].asDict())

    # 4 - Add HelpfulnessRatio column
    df = df.withColumn(
        "HelpfulnessRatio",
        (col("HelpfulnessNumerator") / col("HelpfulnessDenominator")).cast("double")
    )
    df.show(5)
    
    # 5 - Filter rows with HelpfulnessDenominator > 0
    valid_helpfulness_df = df.filter(df["HelpfulnessDenominator"] > 0)
    valid_helpfulness_df.show(5)

    # 6 - Count distinct UserId values
    distinct_users_count = df.select("UserId").distinct().count()
    print(f"Distinct Users: {distinct_users_count}")

    # 20 - Calculate average Score per ProductId
    average_score_df = df.groupBy("ProductId").agg(avg("Score").alias("AverageScore"))
    average_score_df.show(5)


    # ---- SQL ----
    df.createOrReplaceTempView("reviews")

    # 1 - Register the DataFrame as a temporary view
    sql_query = "SELECT * FROM reviews"
    result = spark_session.sql(sql_query)
    result.show()


if __name__ == '__main__':
    start()