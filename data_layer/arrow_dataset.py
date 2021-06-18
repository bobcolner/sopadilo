import pandas as pd
from pyarrow.dataset import dataset, field
from pyarrow._dataset import FileSystemDataset
from pyarrow.fs import S3FileSystem
from utilities import project_globals as g


def get_dataset(prefix: str, symbol: str=None, fs_type: str='local', schema=None) -> FileSystemDataset:

    if symbol is not None:
        path_str = f"{prefix}/symbol={symbol}"
    else:
        path_str = prefix

    if fs_type == 'local':
        ds = dataset(
            source=g.DATA_LOCAL_PATH + path_str,
            format='feather',
            partitioning='hive',
            schema=schema,
            exclude_invalid_files=True
        )
    elif fs_type in ['s3', 'remote']:
        s3  = S3FileSystem(
            access_key=g.B2_APPLICATION_KEY_ID,
            secret_key=g.B2_APPLICATION_KEY,
            endpoint_override=g.B2_ENDPOINT_URL
        )
        ds = dataset(
            source=g.DATA_S3_PATH + path_str,
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
