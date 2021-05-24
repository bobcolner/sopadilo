import pandas as pd
from filters import mad, jma, tick_rule


def filter_trades(tdf: pd.DataFrame, value_winlen: int=22, deviation_winlen: int=1111, k: int=11) -> pd.DataFrame:
    tdf = tdf.copy()
    tdf['status'] = 'clean'
    # filter ts delta
    ts_delta = abs(tdf.sip_dt - tdf.exchange_dt) > pd.to_timedelta(3, unit='S')
    tdf.loc[ts_delta, 'status'] = 'filtered: ts diff'    
    # filter irregular
    tdf.loc[tdf.irregular == True, 'status'] = 'filtered: irregular condition'
    # filter zero volume ticks
    tdf.loc[tdf['size'] == 0, 'status'] = 'filtered: zero volume'
    # add local nyc time
    tdf['nyc_dt'] = tdf['sip_dt']
    tdf = tdf.set_index('nyc_dt').tz_localize('UTC').tz_convert('America/New_York')
    # filter hours
    if False:
        early_id = tdf[dt.time(hour=0, minute=0):dt.time(hour=9, minute=30)].index
        late_id = tdf[dt.time(hour=16, minute=0):dt.time(hour=0, minute=0)].index
        tdf.loc[early_id, 'status'] = 'filtered: pre-market'
        tdf.loc[late_id, 'status'] = 'filtered: post-market'

    tdf = tdf.reset_index()
    # remove/rename columns
    tdf = tdf.drop(columns=['sip_dt', 'exchange_dt', 'sequence', 'trade_id', 'exchange_id', 'irregular', 'conditions'])
    tdf = tdf.rename(columns={'size': 'volume'}) 
    # add mad filter
    tdf = mad.mad_filter_df(tdf, col='price', value_winlen=value_winlen, deviation_winlen=deviation_winlen, k=k)
    tdf.loc[0:(value_winlen * 3), 'status'] = 'filtered: MAD warm-up'
    tdf.loc[tdf.mad_outlier==True, 'status'] = 'filtered: MAD outlier'
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
