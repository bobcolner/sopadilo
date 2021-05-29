import pandas as pd
from data_api import polygon_df
from data_layer import storage_adaptor, arrow_dataset
from utilities import globals_unsafe as g


fs = storage_adaptor.StorageAdaptor('s3_filecache', root_path=g.DATA_S3_PATH)

fs_local = storage_adaptor.StorageAdaptor('local', root_path=g.DATA_LOCAL_PATH)


def get_and_save_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:

    print(symbol, date, 'getting data fron polygon api')
    sdf = polygon_df.get_date_df(symbol, date, tick_type=prefix)
    print(symbol, date, 'putting data to S3/B2')
    fs.write_sdf(sdf, symbol, date, prefix)
    print(symbol, date, 'saving data to local file')
    fs_local.write_sdf(sdf, symbol, date, prefix)
    
    return sdf


def fetch_sdf(symbol: str, date: str, prefix: str) -> pd.DataFrame:

    try:
        print(symbol, date, 'trying to get data from local file...')
        sdf = fs_local.read_sdf(symbol, date, prefix)
    except FileNotFoundError:
        try:
            print(symbol, date, 'trying to get data from s3/b2...')
            sdf = fs.read_sdf(symbol, date, prefix)
        except FileNotFoundError:
            print(symbol, date, 'getting data from polygon API...')
            sdf = polygon_df.get_date_df(symbol, date, tick_type=prefix)
            print(symbol, date, 'saving data to S3/B2...')
            fs.write_sdf(sdf, symbol, date, prefix)
        finally:
            print(symbol, date, 'saving data to local file')
            fs_local.write_sdf(sdf, symbol, date, prefix)

    return sdf


def get_market_daily_df(symbol: str, start_date: str, end_date: str, prefix: str, source: str='local') -> pd.DataFrame:
    from pyarrow.dataset import field

    ds = arrow_dataset.get_dataset(symbol, prefix, fs=source)
    filter_exp = (field('date') >= start_date) & (field('date') <= end_date)
    df = ds.to_table(filter=filter_exp).to_pandas()

    return df
