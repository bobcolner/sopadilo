from pathlib import Path
import pandas as pd
from data_api import polygon_df
from data_model import fsspec_storage as fss, arrow_dataset
from utilities import globals_unsafe as g

# /{ROOT_PATH}/{prefix}/symbol={symbol}/date={date}

def list_symbol_dates(symbol: str, prefix: str) -> str:
    paths = fss.list_fs_path(f"{prefix}/symbol={symbol}")
    return [path.split('date=')[1] for path in paths]


def list_symbols(prefix: str) -> str:
    paths = fss.list_fs_path(prefix)
    return [path.split('symbol=')[1] for path in paths]


def remove_symbol(symbol: str, prefix: str):
    fss.remove_fs_path(f"{prefix}/symbol={symbol}/", recursive=True)


def remove_symbol_date(symbol: str, date: str, prefix: str):
    fss.remove_fs_path(f"{prefix}/symbol={symbol}/date={date}/", recursive=True)


def get_symbol_storage_used(symbol: str, prefix: str) -> dict:
    return fss.list_fs_path_storage_used(f"{prefix}/symbol={symbol}/")


def read_sdf(symbol: str, date: str, prefix: str, columns: list=None) -> pd.DataFrame:
    sdf = fss.read_df_from_fs(f"{prefix}/symbol={symbol}/date={date}/data.feather")
    return sdf


def write_sdf(sdf: pd.DataFrame, symbol: str, date: str, prefix: str):
    fss.write_df_to_fs(sdf, fs_path=f"{prefix}/symbol={symbol}/date={date}")


def read_sdpickle(symbol: str, date: str, prefix: str) -> object:
    sd_obj = fss.read_pickle_from_fs(f"{prefix}/symbol={symbol}/date={date}/data.feather")
    return sd_obj


def write_sdpickle(sd_obj: object, symbol: str, date: str, prefix: str):
    fss.write_pickle_to_fs(sd_obj, fs_path=f"{prefix}/symbol={symbol}/date={date}")


def write_sdf_to_local(sdf: pd.DataFrame, symbol:str, date:str, prefix: str) -> str:
    path = g.DATA_LOCAL_PATH + f"/{prefix}/symbol={symbol}/date={date}/"
    print(path)
    Path(path).mkdir(parents=True, exist_ok=True)
    sdf.to_feather(path+'data.feather', version=2)


def get_and_save_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:
    print(symbol, date, 'getting data fron polygon api')
    sdf = polygon_df.get_date_df(symbol, date, tick_type=prefix)
    print(symbol, date, 'putting data to S3/B2')
    write_sdf(sdf, symbol, date, prefix)
    print(symbol, date, 'saving data to local file')
    path = write_sdf_to_local(sdf, symbol, date, prefix)
    return sdf


def fetch_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:
    try:
        print(symbol, date, 'trying to get data from local file...')
        sdf = pd.read_feather(g.DATA_LOCAL_PATH + f"/{prefix}/symbol={symbol}/date={date}/data.feather")
    except FileNotFoundError:
        try:
            print(symbol, date, 'trying to get data from s3/b2...')
            sdf = read_sdf(symbol, date, prefix)
        except FileNotFoundError:
            print(symbol, date, 'getting data from polygon API...')
            sdf = polygon_df.get_date_df(symbol, date, tick_type=prefix)
            print(symbol, date, 'saving data to S3/B2...')
            write_sdf(sdf, symbol, date, prefix)
        finally:
            print(symbol, date, 'saving data to local file')
            path = write_sdf_to_local(sdf, symbol, date, prefix)

    return sdf


def get_market_daily_df(symbol: str, start_date: str, end_date: str, prefix: str, source: str='local') -> pd.DataFrame:
    
    if source == 'local':
        ds = arrow_dataset.get_local_dataset(symbol, prefix)
    elif source == 's3':
        ds = arrow_dataset.get_s3_dataset(symbol, prefix)
    
    from pyarrow.dataset import field
    filter_exp = (field('date') >= start_date) & (field('date') <= end_date)

    return ds.to_table(filter=filter_exp).to_pandas()
