from icebergray import read_iceberg

DB_NAME = "iceberg_ref"


TEST_TABLE_NAMES = ["iceberg_ref.nested_frame_unpartitioned4"]


if __name__ == "__main__":
    for table_name in TEST_TABLE_NAMES:
        print(read_iceberg(table_name, catalog_name="default").to_pandas())