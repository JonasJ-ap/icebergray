from typing import Optional, List, Dict, Any, Tuple

from pyiceberg.table import Table
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from pyarrow.fs import FileSystem

import numpy as np
from ray.data import read_parquet
from ray.data.dataset import Dataset
from ray.data.datasource import DefaultParquetMetadataProvider


def read_iceberg(
        table_identifier: str,
        *,
        catalog_name: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        case_sensitive: bool = True,
        filesystem: Optional[FileSystem] = None,
        columns: Optional[List[str]] = None,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
        meta_provider=DefaultParquetMetadataProvider(),
        **arrow_parquet_args,
):
    # TODO: in pyiceberg0.4.0, we can have a default-catalog option: https://github.com/apache/iceberg/pull/6864
    catalog = load_catalog(catalog_name) if catalog_name else load_catalog()
    table = catalog.load_table(table_identifier)
    return _read_iceberg(
        table,
        snapshot_id=snapshot_id,
        case_sensitive=case_sensitive,
        filesystem=filesystem,
        columns=columns,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        tensor_column_schema=tensor_column_schema,
        meta_provider=meta_provider,
        **arrow_parquet_args,
    )


def _read_iceberg(
        table: Table,
        *,
        snapshot_id: Optional[int] = None,
        case_sensitive: bool = True,
        filesystem: Optional[FileSystem] = None,
        columns: Optional[List[str]] = None,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
        meta_provider=DefaultParquetMetadataProvider(),
        **arrow_parquet_args,
) -> Dataset:
    table_scan = table.scan(snapshot_id=snapshot_id, case_sensitive=case_sensitive)
    planned_files = [file_task.file.file_path for file_task in table_scan.plan_files()]
    # TODO: the converted schema corrupts partitioned tables, maybe fixed in iceberg#6997
    table_schema = schema_to_pyarrow(table.schema())

    return read_parquet(
        paths=planned_files,
        filesystem=filesystem,
        columns=columns,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        tensor_column_schema=tensor_column_schema,
        meta_provider=meta_provider,
        dataset_kwargs=dict(partitioning=None),  # required since https://github.com/ray-project/ray/issues/21957
        **arrow_parquet_args,
    )
