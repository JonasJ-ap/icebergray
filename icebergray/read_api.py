from typing import Optional, List, Dict, Any, Tuple

from pyiceberg.table import Table
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.io.pyarrow import schema_to_pyarrow

from pyarrow.fs import FileSystem

import numpy as np
from ray.data import read_parquet
from ray.data.dataset import Dataset
from ray.data.datasource import DefaultParquetMetadataProvider

import pyarrow as pa


def read_iceberg(
        table_identifier: str,
        *,
        catalog_name: Optional[str] = None,
        catalog_properties: Optional[Dict[str, str]] = None,
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
    """Create an Arrow datastream from an iceberg table.
       Examples:
             >>> import ray
             >>> # Read an iceberg table via its catalog.
             >>> ray.data.read_iceberg("db.table", catalog_name="demo",
             ...   catalog_properties={"type": "glue"}) # doctest: +SKIP
             For further arguments you can pass to pyarrow as a keyword argument, see
             https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_fragment
       Args:
           table_identifier: Iceberg table identifier. For the format, see details in
               https://py.iceberg.apache.org/api/#load-a-table
           catalog_name: The name of catalog used to load the iceberg table.
           catalog_properties: The configurations used to load the iceberg catalog.
               See details in https://py.iceberg.apache.org/configuration/
           snapshot_id: The snapshot id of iceberg table to read. If not specified,
               the latest snapshot will be read.
           filesystem: The filesystem implementation to read from. These are specified in
               https://arrow.apache.org/docs/python/api/filesystems.html#filesystem-implementations.
           columns: A list of column names to read.
           parallelism: The requested parallelism of the read. Parallelism may be
               limited by the number of files of the datastream.
           ray_remote_args: kwargs passed to ray.remote in the read tasks.
           tensor_column_schema: A dict of column name --> tensor dtype and shape
               mappings for converting a Parquet column containing serialized
               tensors (ndarrays) as their elements to our tensor column extension
               type. This assumes that the tensors were serialized in the raw
               NumPy array format in C-contiguous order (e.g. via
               `arr.tobytes()`).
           meta_provider: File metadata provider. Custom metadata providers may
               be able to resolve file metadata more quickly and/or accurately.
           arrow_parquet_args: Other parquet read options to pass to pyarrow, see
               https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_fragment
       Returns:
           Datastream producing Arrow records read from the specified paths.
       """
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    catalog_properties = catalog_properties or {}
    # in pyiceberg0.4.0, we can have a default-catalog option:
    # https://github.com/apache/iceberg/pull/6864
    catalog = load_catalog(catalog_name, **catalog_properties)
    table = catalog.load_table(table_identifier)
    partition_spec = table.spec()

    planned_files = [
        file_task.file.file_path
        for file_task in table.scan(snapshot_id=snapshot_id).plan_files()
    ]

    partitioning = None
    if partition_spec.fields:
        iceberg_partition_schema = Schema(
            *[
                table.schema().find_field(field.source_id)
                for field in partition_spec.fields
            ]
        )
        # timestamp casting issue fixed in pyiceberg0.4.0:
        # https://github.com/apache/iceberg/pull/6946
        partitioning = pa.dataset.partitioning(
            schema=schema_to_pyarrow(iceberg_partition_schema),
            flavor="hive",
        )

    return read_parquet(
        paths=planned_files,
        filesystem=filesystem,
        columns=columns,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        tensor_column_schema=tensor_column_schema,
        meta_provider=meta_provider,
        dataset_kwargs=dict(
            partitioning=partitioning
        ),  # required since https://github.com/ray-project/ray/issues/21957
        **arrow_parquet_args,
    )
