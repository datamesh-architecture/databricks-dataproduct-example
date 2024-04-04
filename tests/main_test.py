import pandas as pd
from pandas.testing import assert_frame_equal

from stock_last_sales.main import calculate_last_sales, get_spark


# noinspection PyTypeChecker
def test_calculate_last_sales():
    spark = get_spark()
    stock_data = {
        "sku": ["0000000000001", "0000000000002", "0000000000003"],
        "quantity": [1, 2, 3],
        "updated_timestamp": ["2024-03-07T19:27:56.802952", "2024-03-08T19:27:56.802967", "2024-03-04T19:27:56.802970"],
        "processing_timestamp": ["2024-02-29T19:27:56.809050", "2024-03-09T19:27:56.809054", "2024-03-02T19:27:56.809058"]
    }
    stock_df = to_dataframe(spark, stock_data)

    sales_data = {
        "order_id": ["0fff1c4b-bd7e-4c18-963d-f2246b873ade", "9aa9ecf9-d3b5-41bc-810a-09a3c8aedb8a",  "9aa9ecf9-d3b5-41bc-810a-09a3c8aedb8a"],
        "customer_id": ["deca697a-a826-4153-a087-e54c78cdb0ee", "77f327de-adea-4a10-acee-3ed84a3aff02", "77f327de-adea-4a10-acee-3ed84a3aff02"],
        "sku": ["0000000000001", "0000000000001", "0000000000002"],
        "order_timestamp": ["2024-01-01T10:00:00.000000", "2024-01-02T10:00:00.000000", "2024-01-02T10:00:00.000000"],
        "processing_timestamp": ["2024-03-17T19:31:48.501565", "2024-03-05T19:31:48.501570", "2024-03-05T19:31:48.501570"]
    }
    sales_df = to_dataframe(spark, sales_data)

    # call method under test
    last_sales_sdf = calculate_last_sales(spark, stock_df, sales_df)

    expected_data = {
        "sku": ["0000000000001", "0000000000002", "0000000000003"],
        "quantity": [1, 2, 3],
        "processing_timestamp": ["2024-02-29T19:27:56.809050", "2024-03-09T19:27:56.809054", "2024-03-02T19:27:56.809058"],
        "last_sale_timestamp": ["2024-01-02T10:00:00.000000", "2024-01-02T10:00:00.000000", None],
    }
    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(last_sales_sdf.toPandas(), expected_df)


def to_dataframe(spark, data_dict):
    df = pd.DataFrame(data_dict)
    return spark.createDataFrame(df)
