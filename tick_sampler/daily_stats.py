import datetime as dt
import pandas as pd
from filters import jma
from data_layer import data_access


def get_symbol_stats(symbol: str, start_date: str, 
    end_date: str=(dt.datetime.today().date() - dt.timedelta(days=1)).isoformat()) -> pd.DataFrame:

    # get exta 10 days
    adj_start_date = (dt.datetime.fromisoformat(start_date) - dt.timedelta(days=20)).date().isoformat()
    # get market daily from pyarrow dataset
    df = data_access.fetch_market_daily(start_date=adj_start_date, end_date=end_date)
    df = df.loc[df['symbol'] == symbol].reset_index(drop=True)
    # range/volitiliry metric
    df.loc[:, 'range'] = df['high'] - df['low']
    df = jma.jma_filter_df(df, col='range', winlen=5, power=1)
    df.loc[:, 'range_jma_lag'] = df['range_jma'].shift(1)
    # recent price/value metric
    df.loc[:, 'close_lag'] = df['close'].shift(1)
    df = jma.jma_filter_df(df, col='vwap', winlen=7, power=1)
    df.loc[:, 'vwap_jma_lag'] = df['vwap_jma'].shift(1)

    return df.dropna().reset_index(drop=True)
