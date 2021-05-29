import pandas as pd
from pyarrow.dataset import dataset
from pyarrow._dataset import FileSystemDataset
from pyarrow.fs import S3FileSystem
from utilities import globals_unsafe as g


def get_dataset(symbol: str, prefix: str, fs: str='local', schema=None) -> FileSystemDataset:

    if fs == 'local':
        full_path = g.DATA_LOCAL_PATH + f"/{prefix}/"
        if symbol:
            full_path = full_path + f"symbol={symbol}/"
        ds = dataset(
            source=full_path,
            format='feather',
            partitioning='hive',
            schema=schema,
            exclude_invalid_files=True
        )
    elif fs == 's3':
        s3  = S3FileSystem(
            access_key=g.B2_ACCESS_KEY_ID,
            secret_key=g.B2_SECRET_ACCESS_KEY,
            endpoint_override=g.B2_ENDPOINT_URL
        )
        ds = dataset(
            source=g.DATA_S3_PATH + f"/{prefix}/symbol={symbol}/",
            format='feather',
            filesystem=s3,
            schema=schema,
            partitioning='hive',
            exclude_invalid_files=True
        )

    return ds
