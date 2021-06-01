import pandas as pd
from pyarrow.dataset import dataset, field
from pyarrow._dataset import FileSystemDataset
from pyarrow.fs import S3FileSystem
from utilities import project_globals as g


def get_dataset(symbol: str, prefix: str, fs_type: str='local', schema=None) -> FileSystemDataset:

    if fs_type == 'local':
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
    elif fs_type in ['s3', 'remote']:
        s3  = S3FileSystem(
            access_key=g.B2_ACCESS_KEY_ID,
            secret_key=g.B2_SECRET_ACCESS_KEY,
            endpoint_override=g.B2_ENDPOINT_URL
        )
        ds = dataset(
            source=g.DATA_S3_PATH + f"{prefix}/symbol={symbol}/",
            format='feather',
            filesystem=s3,
            schema=schema,
            partitioning='hive',
            exclude_invalid_files=True
        )

    return ds


def get_market_daily_df(symbol: str, start_date: str, end_date: str, prefix: str, source: str='local') -> pd.DataFrame:

    ds = get_dataset(symbol, prefix, fs_type=source)
    filter_exp = (field('date') >= start_date) & (field('date') <= end_date)
    return ds.to_table(filter=filter_exp).to_pandas()
