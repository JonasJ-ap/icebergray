from icebergray import read_iceberg
from pyiceberg.catalog import load_catalog

DB_NAME = "iceberg_ref"

TEST_TABLE_NAMES = ["iceberg_ref.nyc_taxis", "iceberg_ref.iceberg_all_types_parquet", "iceberg_ref.iceberg_nested_parquet"]

catalog = load_catalog("default")

if __name__ == "__main__":
    for table_name in ["iceberg_ref.iceberg_all_types_partitioned_parquet_v1"]:
        print(f"Reading table {table_name}")
        print(read_iceberg(table_name, catalog_name="default").limit(100).to_pandas(limit=100))
