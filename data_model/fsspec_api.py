from io import BytesIO
from tempfile import NamedTemporaryFile
from pathlib import Path
import pandas as pd
from data_api import polygon_df
from data_model import fsspec_backend as fsb
from utilities import pickle, globals_unsafe as gus

# /{ROOT_PATH}/{prefix}/symbol={symbol}/date={date}
# prefix e.g. 'data_ingest/polygon/trades', 'data_index/polygon/trades/id'

fs, ROOT_PATH = fsb.get_fsspec_fs(fs_type='s3_cached')


def list_symbol_dates(symbol: str, prefix: str) -> str:
    paths = fs.ls(path=ROOT_PATH + f"/{prefix}/symbol={symbol}/", refresh=True)
    # paths = fsb.list_fs_path(fs_path=f"data/{prefix}/symbol={symbol}/")
    return [path.split('date=')[1] for path in paths]


def list_symbols(prefix: str) -> str:
    paths = fs.ls(path=ROOT_PATH + f"/{prefix}", refresh=True)
    # paths = fsb.list_fs_path(f"data/{prefix}")
    return [path.split('symbol=')[1] for path in paths]


def remove_symbol(symbol: str, prefix: str):
    path = ROOT_PATH + f"/{prefix}/symbol={symbol}/"
    fs.rm(path, recursive=True)


def get_symbol_storage_used(symbol: str, prefix: str) -> dict:
    path = ROOT_PATH + f"/{prefix}/symbol={symbol}/"
    return fs.du(path)


def get_sdf(symbol: str, date: str, prefix: str, columns: list=None) -> pd.DataFrame:
    sdf = read_sdf_from_fs(
        fs_path=f"/{prefix}/symbol={symbol}/date={date}/data.feather"
        )
    return sdf


def write_sdf(sdf: pd.DataFrame, symbol: str, date: str, prefix: str):
    fsb.df_to_fs(
        df=df, 
        fs_path=f"/{prefix}/symbol={symbol}/date={date}"
        )


def sdf_to_file(sdf: pd.DataFrame, symbol:str, date:str, prefix: str) -> str:
    path = ROOT_PATH + f"/{prefix}/symbol={symbol}/date={date}/"
    Path(path).mkdir(parents=True, exist_ok=True)
    df.to_feather(path+'data.feather', version=2)
    return path + 'data.feather'


def get_and_save_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:
    print(symbol, date, 'getting data fron polygon api')
    df = polygon_df.get_sdf(symbol, date, prefix)
    print(symbol, date, 'putting data to S3/B2')
    put_sdf_to_fs(df, symbol, date, prefix)
    print(symbol, date, 'saving data to local file')
    path = sdf_to_file(df, symbol, date, prefix)
    return df


def fetch_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:
    try:
        print(symbol, date, 'trying to get data from local file...')
        df = pd.read_feather(gus.DATA_LOCAL_PATH + f"/{prefix}/symbol={symbol}/date={date}/data.feather")
    except FileNotFoundError:
        try:
            print(symbol, date, 'trying to get data from s3/b2...')
            df = get_sdf_from_fs(symbol, date, prefix)
        except FileNotFoundError:
            print(symbol, date, 'getting data from polygon API...')
            df = polygon_df.get_sdf(symbol, date, prefix)
            print(symbol, date, 'saving data to S3/B2...')
            put_sdf_to_fs(df, symbol, date, prefix)
        finally:
            print(symbol, date, 'saving data to local file')
            path = sdf_to_file(df, symbol, date, prefix)

    return df
