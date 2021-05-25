import datetime as dt
import pandas as pd
from tqdm import tqdm
from data_model import arrow_dataset, s3_backend
from tick_filter import streaming_tick_filter, batch_tick_filer
from tick_timebins import batch_timebins
from tick_sampler import streaming_tick_sampler, labels


def persist_sample_date(thresh: dict, date: str):

    bar_date = sample_date(thresh, date)
    del bar_date['ticks_df']
    put_pickle_to_s3(obj=bar_date, s3_path=f"samples/v1/symbol={thresh['meta']['symbol']}/date={date}/")

def sample_date(thresh: dict, date: str):

    tick_filter = streaming_tick_filter.StreamingTickFilter(**thresh['filter'])
    tick_sampler = streaming_tick_sampler.StreamingTickSampler(thresh['sampler'])
    # get raw trades
    tdf = s3_backend.fetch_date_df(symbol=thresh['meta']['symbol'], date=date, tick_type='trades')
    for tick in tqdm(tdf.itertuples(), total=tdf.shape[0], disable=True):
        # filter/enrich tick
        tick_filter.update(
            price=tick.price,
            volume=tick.size,
            sip_dt=tick.sip_dt,
            exchange_dt=tick.exchange_dt,
            conditions=tick.conditions
        )
        # get current 'filtered' tick
        ftick = tick_filter.ticks[-1]
        # sample 'clean' ticks
        if ftick['status'] == 'clean: market-open':
            # timebin [todo]
            # ...
            # sample ticks as bars
            tick_sampler.update(
                close_at=ftick['nyc_dt'],
                price=ftick['price'],
                volume=ftick['volume'],
                side=ftick['side'],
                price_jma=ftick['price_jma']
            )

    # get processed ticks
    tdf = pd.DataFrame(tick_filter.ticks)
    # label bars
    if thresh['sampler']['add_label']:
        bars = labels.label_bars(
            bars=tick_sampler.bars,
            ticks_df=tdf[tdf.status.str.startswith('clean: market-open')],
            risk_level=thresh['sampler']['renko_size'],
            horizon_mins=thresh['sampler']['max_duration_td'].total_seconds() / 60,
            reward_ratios=thresh['sampler']['reward_ratios'],
            )

    bar_date = {
        'symbol': thresh['meta']['symbol'],
        'date': date,
        'thresh': thresh,
        'ticks_df': tdf,
        'filtered_df': tdf.loc[tdf['status'].str.startswith('filtered')].status,
        'bars_df': pd.DataFrame(bars),
        'bars': bars,
        }

    return bar_date


def sample_date_batch(thresh: dict, date: str) -> dict:
    # get raw ticks (all trades)
    tdf_v1 = s3_backend.fetch_date_df(thresh['meta']['symbol'], date=date, tick_type='trades')
    # MAD filter ticks (all trades)
    tdf_v2 = batch_tick_filer.filter_trades(tdf_v1,
        thresh['filter']['mad_value_winlen'],
        thresh['filter']['mad_deviation_winlen'],
        thresh['filter']['mad_k']
        )
    # drop dirtly trades (clean only)
    tdf_v3 = tdf_v2[~tdf_v2.status.str.startswith('filtered')]
    # enrich with tick-rule and jma (clean only)
    tdf_v4 = batch_tick_filer.enrich_tick(tdf_v3)
    # combine ticks (all trades)
    tdf_v5 = pd.merge(
        left=tdf_v2[['nyc_dt', 'price', 'volume', 'status', 'price_median_diff_median']], 
        right=tdf_v4[['nyc_dt', 'price', 'volume', 'price_jma']],
        on=['nyc_dt', 'price', 'volume'],
        how='left',
        )
    # time bactch ticks
    bdf = batch_timebins.get_timebins(tdf_v4, freq=thresh['filter']['batch_freq'])
    # sample bars
    tick_sampler = streaming_tick_sampler.StreamingTickSampler(thresh)
    tick_sampler.batch_update(bdf)

    # label bars
    if thresh['sampler']['add_label']:
        bars = labels.label_bars(
            bars=tick_sampler.bars,
            ticks_df=tdf_v4,
            risk_level=thresh['sampler']['renko_size'],
            horizon_mins=thresh['sampler']['max_duration_td'].total_seconds() / 60,
            reward_ratios=thresh['sampler']['reward_ratios'],
            )

    bar_date = {
        'symbol': thresh['meta']['symbol'],
        'date': date,
        'thresh': thresh,
        'ticks_df': tdf_v5,
        'timebins_df': bdf,
        'bars_df': pd.DataFrame(bars),
        'bars': bars,
        }

    return bar_date

