import datetime as dt
import pandas as pd
from data_model import arrow_dataset, s3_backend
from filters import mad, jma, tick_rule
from bar_sampler import time_batches, sampler, labels


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


def get_filter_and_batch_trades(symbol: str, date: str, freq: str=None) -> pd.DataFrame:
    
    # get raw ticks
    tdf = s3_backend.fetch_date_df(symbol, date, tick_type='trades')
    
    # filter ts delta
    ts_delta = abs(tdf.sip_dt - tdf.exchange_dt) < pd.to_timedelta(3, unit='S')
    tdf = tdf[ts_delta]
    
    # filter irregular
    # irregular_conditions = [2, 5, 7, 10, 13, 15, 16, 20, 21, 22, 29, 33, 38, 52, 53]
    # pd.Series(tick.conditions).isin(irregular_conditions).any():  # 'irrgular' tick condition
    tdf = tdf[tdf.irregular == False]
    
    # add local nyc time
    tdf['nyc_dt'] = tdf['sip_dt']
    tdf = tdf.set_index('nyc_dt').tz_localize('UTC').tz_convert('America/New_York')
    
    # filter pre/post market hours
    tdf = tdf.between_time('9:00:00', '16:00:00').reset_index()
    
    # remove/rename columns
    tdf = tdf.drop(columns=['sip_dt', 'exchange_dt', 'sequence', 'trade_id', 'exchange_id', 'irregular', 'conditions'])
    tdf = tdf.rename(columns={"size": "volume"}) 
    
    # add mad filter
    tdf = mad.mad_filter_df(tdf, col='price', center=False, value_winlen=22, devations_winlen=1111, k=11, diff='pct')
    
    # add tick-rule trade 'side' (and filter mad outliers)
    tick_rule_filter = tick_rule.TickRule()
    jma_filter = jma.JMAFilter(winlen=7, power=2)
    rows = []
    for row in tdf[tdf.mad_outlier==False].itertuples():  # use clean ticks
        rows.append({
            'nyc_dt': row.nyc_dt,
            'price': row.price,
            'price_jma': jma_filter.update(row.price),
            'volume': row.volume,
            'side': tick_rule_filter.update(row.price)
        })
    tdf = pd.DataFrame(rows)

    # time-based batches
    if freq:
        bdf = time_batches.get_batches(tdf, freq=freq)
    else:
        bdf = None
    
    return tdf, bdf


def get_bar_date(thresh: dict, date: str) -> dict:
    
    # get ticks
    tdf, bdf = get_filter_and_batch_trades(symbol=thresh['symbol'], date=date, freq='3s')
    
    # sample bars
    bar_sampler = sampler.BarSampler(thresh)
    for row in bdf.dropna().itertuples():
        bar_sampler.update(row)
    bars = bar_sampler.bars
    
    # label bars
    if thresh['add_label']:
        bars = labels.label_bars(
            bars=bars,
            ticks_df=tdf,
            risk_level=thresh['renko_size'],
            horizon_mins=thresh['max_duration_td'].total_seconds() / 60,
            reward_ratios=thresh['reward_ratios'],
            )
    # package output        
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

    return bar_dates
