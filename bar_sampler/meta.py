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


def filter_trades(tdf: pd.DataFrame) -> pd.DataFrame:

    tdf['status'] = 'clean'

    # filter ts delta
    ts_delta = abs(tdf.sip_dt - tdf.exchange_dt) > pd.to_timedelta(3, unit='S')
    tdf.loc[ts_delta, 'status'] = 'filtered: ts diff'
    
    # filter irregular
    tdf.loc[tdf.irregular == True, 'status'] = 'filtered: irregular conditions'
    # irregular_conditions = [2, 5, 7, 10, 13, 15, 16, 20, 21, 22, 29, 33, 38, 52, 53]
    # pd.Series(tick.conditions).isin(irregular_conditions).any():  # 'irrgular' tick condition
    
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

    return tdf


def enrich_tick(tdf: pd.DataFrame) -> pd.DataFrame:
    tick_rule_filter = tick_rule.TickRule()
    jma_filter = jma.JMAFilter(winlen=7, power=2)
    rows = []
    # for row in tdf.loc[~tdf.status.str.startswith('filtered:')].itertuples():  # use clean ticks
    for row in tdf.itertuples():  # use all ticks
        if row.status.startswith('filtered:') != True:
            jma_value = jma_filter.update(row.price)
            tick_side = tick_rule_filter.update(row.price)
        else:
            jma_value = None
            tick_side = None
        clean_tick = {
            'nyc_dt': row.nyc_dt,
            'price': row.price,
            'price_jma': jma_value,
            'volume': row.volume,
            'side': tick_side,
            'status': row.status
        }
        rows.append(clean_tick)

    return pd.DataFrame(rows)


def get_bar_date(thresh: dict, date: str) -> dict:
    # get raw ticks
    tdf = s3_backend.fetch_date_df(thresh['symbol'], date, tick_type='trades')
    # filter ticks
    tdf = filter_trades(tdf)
    # enrich with tick-rule and jma 
    tdf = enrich_tick(tdf)
    # time bactch ticks
    bdf = time_batches.get_batches(tdf, freq=thresh['batch_freq'])
    # sample bars
    bar_sampler = sampler.BarSampler(thresh)
    # for row in bdf.dropna().itertuples():
    for row in bdf.itertuples():
        bar_sampler.update(row)
    
    # label bars
    if thresh['add_label']:
        bars = labels.label_bars(
            bars=bar_sampler.bars,
            ticks_df=tdf,
            risk_level=thresh['renko_size'],
            horizon_mins=thresh['max_duration_td'].total_seconds() / 60,
            reward_ratios=thresh['reward_ratios'],
            )

    bar_date = {
        'symbol': thresh['symbol'],
        'date': date,
        'thresh': thresh,
        'ticks_df': tdf,
        'batches_df': bdf,
        'bars_df': pd.DataFrame(bars),
        'bars': bars,
        }
    return bar_date


def get_bar_dates(thresh: dict) -> list:

    daily_stats_df = get_symbol_vol_filter(thresh['symbol'], thresh['start_date'], thresh['end_date'])
    bar_dates = []
    for row in daily_stats_df.itertuples():
        if 'range_jma_lag' in daily_stats_df.columns:
            rs = max(row.range_jma_lag / thresh['renko_range_frac'], row.vwap_jma_lag * 0.0005)  # force min
            rs = min(rs, row.vwap_jma_lag * 0.005)  # enforce max
            thresh.update({'renko_size': rs})

        bar_date = get_bar_date(thresh, row.date)
        bar_dates.append(bar_date)
    
    stacked_df = stacked.fill_gaps_dates(bar_dates, fill_col='price_vwap')
    stats_df = stacked.stacked_df_stats(stacked_df)

    return bar_dates
