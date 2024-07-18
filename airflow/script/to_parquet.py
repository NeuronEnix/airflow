from pyspark.sql import SparkSession

def csv_to_parquet(input_csv, output_parquet):
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("CSV to Parquet") \
        .config("spark.master", "local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Read the CSV file
    df = spark.read.csv(input_csv, header=True, inferSchema=True)

    # Coalesce to a single partition and write the DataFrame to Parquet format
    df.coalesce(1).write.parquet(output_parquet)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    input_csv = "/app/data/chess_games.csv"
    output_parquet = "/app/data/chess_games.parquet"
    csv_to_parquet(input_csv, output_parquet)
