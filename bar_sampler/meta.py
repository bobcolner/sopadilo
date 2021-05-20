import datetime as dt
import pandas as pd
from data_model import arrow_dataset, s3_backend
from filters import mad, jma, tick_rule
from bar_sampler import time_batches, sampler, labels, stacked


def get_symbol_vol_filter(symbol: str, start_date: str, 
    end_date: str=(dt.datetime.today().date() - dt.timedelta(days=1)).isoformat()) -> pd.DataFrame:

    # get exta 10 days
    adj_start_date = (dt.datetime.fromisoformat(start_date) - dt.timedelta(days=10)).date().isoformat()
    # get market daily from pyarrow dataset
    df = arrow_dataset.get_dates_df(symbol='market', tick_type='daily', start_date=adj_start_date, end_date=end_date, source='local')
    df = df.loc[df['symbol'] == symbol].reset_index(drop=True)
    # range/volitiliry metric
    df.loc[:, 'range'] = df['high'] - df['low']
    df = jma.jma_filter_df(df, col='range', winlen=5, power=1)
    df.loc[:, 'range_jma_lag'] = df['range_jma'].shift(1)
    # recent price/value metric
    df.loc[:, 'price_close_lag'] = df['close'].shift(1)
    df = jma.jma_filter_df(df, col='vwap', winlen=7, power=1)
    df.loc[:, 'vwap_jma_lag'] = df['vwap_jma'].shift(1)
    return df.dropna().reset_index(drop=True)


def filter_trades(tdf: pd.DataFrame, drop_filtered: bool=False) -> pd.DataFrame:
    tdf = tdf.copy()
    tdf['status'] = 'clean'
    # filter ts delta
    ts_delta = abs(tdf.sip_dt - tdf.exchange_dt) > pd.to_timedelta(3, unit='S')
    tdf.loc[ts_delta, 'status'] = 'filtered: ts diff'    
    # filter irregular
    tdf.loc[tdf.irregular == True, 'status'] = 'filtered: irregular conditions'
    # filter zero volume ticks
    tdf.loc[tdf['size'] == 0, 'status'] = 'filtered: zero volume'
    # add local nyc time
    tdf['nyc_dt'] = tdf['sip_dt']
    tdf = tdf.set_index('nyc_dt').tz_localize('UTC').tz_convert('America/New_York').reset_index()
    # filter hours
    if False:
        early_id = df[dt.time(hour=0, minute=0):dt.time(hour=9, minute=31)].index
        late_id = df[dt.time(hour=15, minute=59):dt.time(hour=23, minute=59)].index
        df.loc[early_id, 'status'] = 'filtered: pre-market'
        df.loc[late_id, 'status'] = 'filtered: post-market'
    # remove/rename columns
    tdf = tdf.drop(columns=['sip_dt', 'exchange_dt', 'sequence', 'trade_id', 'exchange_id', 'irregular', 'conditions'])
    tdf = tdf.rename(columns={'size': 'volume'}) 
    # add mad filter
    tdf = mad.mad_filter_df(tdf, col='price', center=False, value_winlen=22, devations_winlen=1111, k=11, diff='pct')
    tdf.loc[tdf.mad_outlier==True, 'status'] = 'filtered: MAD outlier'
    tdf.loc[0:20, 'status'] = 'filtered: MAD warm-up'
    # optional drop bad/filtered rows
    if drop_filtered:
        tdf = tdf.loc[tdf.status.str.startswith('filtered:') != True]

    return tdf


def enrich_tick(tdf: pd.DataFrame) -> pd.DataFrame:
    tdf = tdf.copy()
    tick_rule_filter = tick_rule.TickRule()
    jma_filter = jma.JMAFilter(winlen=7, power=2)
    rows = []
    for row in tdf.itertuples():
        tick = {
            'nyc_dt': row.nyc_dt,
            'price': row.price,
            'price_jma': jma_filter.update(row.price),
            'volume': row.volume,
            'side': tick_rule_filter.update(row.price),
            'status': row.status,
        }
        rows.append(tick)

    return pd.DataFrame(rows)


def get_bar_date(thresh: dict, date: str) -> dict:
    # get raw ticks
    tdf_v1 = s3_backend.fetch_date_df(thresh['symbol'], date, tick_type='trades')
    # filter ticks
    tdf_v2 = filter_trades(tdf_v1, drop_filtered=False)
    # drop dirtly trades
    tdf_v3 = tdf_v2[~tdf_v2.status.str.startswith('filtered')]
    # enrich with tick-rule and jma 
    tdf_v4 = enrich_tick(tdf_v3)
    # time bactch ticks
    bdf = time_batches.get_batches(tdf_v4, freq=thresh['batch_freq'])
    # sample bars
    bar_sampler = sampler.BarSampler(thresh)
    bar_sampler.batch(bdf)

    # label bars
    if thresh['add_label']:
        bars = labels.label_bars(
            bars=bar_sampler.bars,
            ticks_df=tdf_v4,
            risk_level=thresh['renko_size'],
            horizon_mins=thresh['max_duration_td'].total_seconds() / 60,
            reward_ratios=thresh['reward_ratios'],
            )

    bar_date = {
        'symbol': thresh['symbol'],
        'date': date,
        'thresh': thresh,
        'ticks_df_all': tdf_v2,
        'ticks_df_clean': tdf_v4,
        'batches_df': bdf,
        'bars_df': pd.DataFrame(bars),
        'bars': bars,
        }
    return bar_date


def get_bar_dates(thresh: dict, ray_on: bool=True) -> list:

    daily_stats_df = get_symbol_vol_filter(thresh['symbol'], thresh['start_date'], thresh['end_date'])
    bar_dates = []
    if ray_on:
        import ray
        ray.init(dashboard_port=1111, ignore_reinit_error=True)
        get_bar_date_ray = ray.remote(get_bar_date)

    for row in daily_stats_df.itertuples():
        if 'range_jma_lag' in daily_stats_df.columns:
            rs = max(row.range_jma_lag / thresh['renko_range_frac'], row.vwap_jma_lag * 0.0005)  # force min
            rs = min(rs, row.vwap_jma_lag * 0.005)  # enforce max
            thresh.update({'renko_size': rs})

        if ray_on:
            bar_date = get_bar_date_ray.remote(thresh, row.date)
        else:
            bar_date = get_bar_date(thresh, row.date)

        bar_dates.append(bar_date)

    if ray_on:
        bar_dates = ray.get(bar_dates)
    
    # stacked_df = stacked.fill_gaps_dates(bar_dates, fill_col='price_vwap')
    # stats_df = stacked.stacked_df_stats(stacked_df)

    return bar_dates
