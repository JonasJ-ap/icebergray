from icebergray import read_iceberg

DB_NAME = "iceberg_ref"

TEST_TABLE_NAMES = [
                    "iceberg_ref.iceberg_all_types_parquet",
                    "iceberg_ref.iceberg_nested_parquet",
                    "iceberg_ref.iceberg_all_primitive_types_id_bool_partitioned_parquet",
                    "iceberg_ref.iceberg_all_primitive_types_id_bool_partitioned_parquet",
                    "iceberg_ref.iceberg_all_primitive_types_id_partitioned_parquet",
                    "iceberg_ref.iceberg_all_types_id_partitioned_parquet",
                    "iceberg_ref.iceberg_all_types_partitioned_parquet_v1",
                    "iceberg_ref.iceberg_nested_partitioned_parquet",
                    "iceberg_ref.iceberg_nested_partitioned_parquet_v1",
                    "snapshot_to_iceberg_demo.migrated_iceberg_all_types_partitioned_same_location"
                    ]


if __name__ == "__main__":
    for table_name in TEST_TABLE_NAMES:
        print(f"Reading table {table_name}")
        print(read_iceberg(table_name, catalog_name="default").limit(100).to_pandas(limit=100))
