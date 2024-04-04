from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max


def get_sales(spark: SparkSession) -> DataFrame:
    return spark.read.table("acme.stock_last_sales.input_sales")


def get_current_stock(spark: SparkSession) -> DataFrame:
    return spark.read.table("acme.stock_last_sales.input_stock")


def write_to_table(spark: SparkSession, df: DataFrame, table_name: str):
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Written to {table_name}")
    return


# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def calculate_last_sales(spark: SparkSession, stock: DataFrame,
    sales: DataFrame) -> DataFrame:
    last_sales_timestamp = sales.groupBy("sku") \
        .agg(max("order_timestamp").alias("last_sale_timestamp"))

    stock_with_last_sale_timestamp = (stock
                                      .join(last_sales_timestamp, on="sku", how="left")
                                      .drop("updated_timestamp")
                                      )
    return stock_with_last_sale_timestamp


def main():
    spark = get_spark()
    current_stock_df = get_current_stock(spark)
    sales_df = get_sales(spark)

    stock_last_sales_sdf = calculate_last_sales(spark, current_stock_df, sales_df)

    write_to_table(spark, stock_last_sales_sdf,"acme.stock_last_sales.articles")


if __name__ == '__main__':
    main()
