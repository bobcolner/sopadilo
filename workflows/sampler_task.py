import datetime as dt
import pandas as pd
from tqdm import tqdm
from data_layer import data_access
from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, labels


def sample_date(config: dict, date: str, progress_bar: bool=True) -> dict:
    
    print('running', config['meta']['symbol'], date)  # logging

    tick_filter = streaming_tick_filter.StreamingTickFilter(**config['filter'])
    tick_sampler = streaming_tick_sampler.StreamingTickSampler(config['sampler'])
    # get raw trades
    tdf = data_access.fetch_sd_data(symbol=config['meta']['symbol'], date=date, prefix='/data/trades')
    for tick in tqdm(tdf.itertuples(), total=tdf.shape[0], disable=(not progress_bar)):
        # filter/enrich tick
        tick_filter.update(
            price=tick.price,
            volume=tick.size,
            sip_dt=tick.sip_dt,
            exchange_dt=tick.exchange_dt,
            conditions=tick.conditions,  # convert to list (fix bug when run in parallel)
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
    if config['meta']['presist_destination'] in ['remote', 'local', 'both']:
        presist_output(bar_date, date, destination=config['meta']['presist_destination'])

    return bar_date


def presist_output(bar_date: dict, date: str, destination: str='remote'):

    # save bars_df
    data_access.presist_sd_data(
        sd_data=bar_date['bars_df'], 
        symbol=bar_date['config']['meta']['symbol'], 
        date=date, 
        prefix=f"/tick_samples/{bar_date['config']['meta']['config_id']}/bars_df",
        destination=destination,
        )
    # drop dataframes
    bd = bar_date.copy()  # copy to avoid mutating date
    del bd['bars_df']
    del bd['ticks_df']
    # save full results
    data_access.presist_sd_data(
        sd_data=bd,
        symbol=bar_date['config']['meta']['symbol'],
        date=date,
        prefix=f"/tick_samples/{bar_date['config']['meta']['config_id']}/bar_date",
        destination=destination,
        )
