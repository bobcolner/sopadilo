from typing import Union
import pandas as pd
from data_layer import storage_adaptor, arrow_dataset
from utilities import globals_unsafe as g


fs_remote = storage_adaptor.StorageAdaptor('s3_filecache', root_path=g.DATA_S3_PATH)

fs_local = storage_adaptor.StorageAdaptor('local', root_path=g.DATA_LOCAL_PATH)


def list_symbol_dates_from_remote(symbol: str, prefix: str):
    return fs_remote.list_symbol_dates(symbol, prefix)


def presist(sd_obj: Union[object, pd.DataFrame], symbol: str, date: str, prefix: str):

    if type(sd_obj) == pd.DataFrame:
        fs_local.write_sdf(sdf, symbol, date, prefix)
        fs_remote.write_sdf(sdf, symbol, date, prefix)
    else:
        fs_local.write_sdpickle(sdf, symbol, date, prefix)
        fs_remote.write_sdpickle(sdf, symbol, date, prefix)


def fetch_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:

    try:
        print(symbol, date, prefix, 'trying to get data from local file...')
        sdf = fs_local.read_sdf(symbol, date, prefix)
    except FileNotFoundError:
        try:
            print(symbol, date, prefix, 'trying to get data from s3/b2...')
            sdf = fs_remote.read_sdf(symbol, date, prefix)
        except FileNotFoundError:
            print(symbol, date, prefix, 'data not found')

    return sdf


def fetch_sdpickle(symbol: str, date: str, prefix: str) -> object:

    try:
        print(symbol, date, prefix, 'trying to get data from local file...')
        sdf = fs_local.read_sdpickle(symbol, date, prefix)
    except FileNotFoundError:
        try:
            print(symbol, date, prefix, 'trying to get data from s3/b2...')
            sdf = fs_remote.read_sdpickle(symbol, date, prefix)
        except FileNotFoundError:
            print(symbol, date, prefix, 'data not found')

    return obj


def fetch_polygon_data(symbol: str, date: str, tick_type: str='trades', prefix: str='/data') -> pd.DataFrame:

    try:
        print(symbol, date, 'trying to get data from local file...')
        sdf = fs_local.read_sdf(symbol, date, prefix=f"{prefix}/{tick_type}")
    except FileNotFoundError:
        try:
            print(symbol, date, 'trying to get data from s3/b2...')
            sdf = fs_remote.read_sdf(symbol, date, prefix=f"{prefix}/{tick_type}")
        except FileNotFoundError:
            print(symbol, date, 'getting data from polygon API...')
            sdf = polygon_df.get_date_df(symbol, date, tick_type)
            print(symbol, date, 'saving data to S3/B2...')
            fs_remote.write_sdf(sdf, symbol, date, prefix=f"{prefix}/{tick_type}")
        finally:
            print(symbol, date, 'saving data to local file')
            fs_local.write_sdf(sdf, symbol, date, prefix=f"{prefix}/{tick_type}")

    return sdf


def get_market_daily_df(symbol: str, start_date: str, end_date: str, prefix: str, source: str='local') -> pd.DataFrame:
    from pyarrow.dataset import field

    ds = arrow_dataset.get_dataset(symbol, prefix, fs=source)
    filter_exp = (field('date') >= start_date) & (field('date') <= end_date)
    df = ds.to_table(filter=filter_exp).to_pandas()

    return df
