import datetime as dt
from tqdm import tqdm
import pandas as pd


def get_batches(tdf: pd.DataFrame, freq: str='3s') -> list:

    def weighted_mean(values: pd.Series, weights: pd.Series) -> pd.Series:
        return (values * weights).sum() / weights.sum()

    date = tdf['nyc_dt'].dt.date[0].isoformat()
    dr = pd.date_range(
        start=f"{date}T09:31:00",
        end=f"{date}T15:59:00",
        freq=freq,
        tz='America/New_York',
        closed=None
        )
    batches = []
    for i in tqdm(list(range(len(dr)-1))):
        tick_batch = tdf.loc[(tdf['nyc_dt'] >= dr[i]) & (tdf['nyc_dt'] < dr[i+1])]
        batch = {}
        batch['open_at'] = dr[i]
        batch['close_at'] = dr[i+1]
        batch['tick_count'] = len(tick_batch)
        batch['volume'] = tick_batch.volume.sum()
        batch['side'] = tick_batch.side.sum()
        if batch['tick_count'] == 1:
            batch['price'] = tick_batch.price.values[0]
            batch['price_jma'] = tick_batch.price_jma.values[0]
            batch['price_high'] = tick_batch.price.values[0]
            batch['price_low'] = tick_batch.price.values[0]
            batch['price_range'] = 0
        elif batch['tick_count'] > 1 :
            batch['price'] = weighted_mean(values=tick_batch.price, weights=tick_batch.volume)
            batch['price_jma'] = weighted_mean(values=tick_batch.price_jma, weights=tick_batch.volume)
            batch['price_high'] = tick_batch.price.max()
            batch['price_low'] = tick_batch.price.min()
            batch['price_range'] = batch['price_high'] - batch['price_low']

        batches.append(batch)

    return pd.DataFrame(batches)


def trunc_epoch(tdf: pd.DataFrame, trunc_list: list=['sec'], dt_col: str='nyc_dt') -> pd.DataFrame:

    if dt_col:
        tdf['epoch'] = tdf[dt_col].values.tolist()  # silent utc conversion
    if 'micro' in trunc_list:
        tdf['epoch_micro'] = tdf['epoch'].floordiv(10 ** 3)
    if 'ms' in trunc_list:
        tdf['epoch_ms'] = tdf['epoch'].floordiv(10 ** 6)
    if 'cs' in trunc_list:
        tdf['epoch_cs'] = tdf['epoch'].floordiv(10 ** 7)
    if 'ds' in trunc_list:
        tdf['epoch_ds'] = tdf['epoch'].floordiv(10 ** 8)
    if 'sec' in trunc_list:
        tdf['epoch_sec'] = tdf['epoch'].floordiv(10 ** 9)
    if 'sec3' in trunc_list:
        tdf['epoch_sec3'] = tdf['epoch'].floordiv((10 ** 9) * 3)
    if 'min' in trunc_list:
        tdf['epoch_min'] = tdf['epoch'].floordiv((10 ** 9) * 60)
    if 'min5' in trunc_list:
        tdf['epoch_min5'] = tdf['epoch'].floordiv((10 ** 9) * 60 * 5)
    if 'hour' in trunc_list:
        tdf['epoch_hour'] = tdf['epoch'].floordiv((10 ** 9) * 60 * 60)

    return tdf


def trunc_datetime(tdf: pd.DataFrame, trunc_list: list=['min'], dt_col: str='nyc_dt') -> pd.DataFrame:

    if 'ms' in trunc_list:
        tdf[dt_col+'_ms'] = tdf[dt_col].values.astype('<M8[ms]')
    if 'sec' in trunc_list:
        tdf[dt_col+'_sec'] = tdf[dt_col].values.astype('<M8[s]')
    if 'min' in trunc_list:
        tdf[dt_col+'_min'] = tdf[dt_col].values.astype('<M8[m]')
    if 'hour' in trunc_list:
        tdf[dt_col+'_hour'] = tdf[dt_col].values.astype('<M8[h]')

    return tdf


def ts_groupby(tdf: pd.DataFrame, column='epoch_cs') -> pd.DataFrame:
    groups = tdf.groupby(column, as_index=False).agg({'price': ['count', 'mean', 'median'], 'volume':'sum'})
    groups.columns = ['_'.join(tup).rstrip('_') for tup in groups.columns.values]
    return groups
