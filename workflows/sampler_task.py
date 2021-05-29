import datetime as dt
import pandas as pd
from tqdm import tqdm
from data_layer import storage_adaptor
from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, labels
from utilities import globals_unsafe as g


db = storage_adaptor.StorageAdaptor(fs_type='s3_filecache', root_path=g.DATA_S3_PATH)


def sample_date(config: dict, date: str, save_results_flag: bool=False) -> dict:
    
    print('running', config['meta']['symbol'], date)  # logging

    tick_filter = streaming_tick_filter.StreamingTickFilter(**config['filter'])
    tick_sampler = streaming_tick_sampler.StreamingTickSampler(config['sampler'])
    # get raw trades
    tdf = db.read_sdf(symbol=config['meta']['symbol'], date=date, prefix='/data/trades')
    for tick in tqdm(tdf.itertuples(), total=tdf.shape[0], disable=True):
        # filter/enrich tick
        tick_filter.update(
            price=tick.price,
            volume=tick.size,
            sip_dt=tick.sip_dt,
            exchange_dt=tick.exchange_dt,
            conditions=tick.conditions,
        )
        # get current 'filtered' tick
        ftick = tick_filter.ticks[-1]
        # sample 'clean' ticks
        if ftick['status'] == 'clean: market-open':
            # sample ticks as bars
            tick_sampler.update(
                close_at=ftick['nyc_dt'],
                price=ftick['price'],
                volume=ftick['volume'],
                side=ftick['side'],
                price_jma=ftick['price_jma'],
            )
    # get processed ticks
    tdf = pd.DataFrame(tick_filter.ticks)
    # label bars
    if config['sampler']['add_label']:
        bars = labels.label_bars(
            bars=tick_sampler.bars,
            ticks_df=tdf[tdf.status.str.startswith('clean: market-open')],
            risk_level=config['sampler']['renko_size'],
            horizon_mins=config['sampler']['max_duration_td'].total_seconds() / 60,
            reward_ratios=config['sampler']['reward_ratios'],
            )

    bar_date = {
        'symbol': config['meta']['symbol'],
        'date': date,
        'config': config,
        'ticks_df': tdf,
        'filtered_df': tdf.loc[tdf['status'].str.startswith('filtered')].status,
        'bars_df': pd.DataFrame(bars),
        'bars': bars,
        }
    if save_results_flag:
        presist_output(bar_date, date)

    return bar_date


def presist_output(bar_date: dict, date: str):

    # save bars_df
    db.write_sdf(
        sdf=bar_date['bars_df'], 
        symbol=bar_date['config']['meta']['symbol'], 
        date=date, 
        prefix=f"/tick_samples/{bar_date['config']['meta']['config_id']}/bars_df",
        )
    # drop dataframes
    bd = bar_date.copy()  # copy to avoid mutating date
    del bd['bars_df']
    del bd['ticks_df']
    # save full results
    db.write_sdpickle(
        sd_obj=bd,
        symbol=bd['config']['meta']['symbol'],
        date=date, 
        prefix=f"/tick_samples/{bd['config']['meta']['config_id']}/bar_date",
        )
