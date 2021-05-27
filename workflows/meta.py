import datetime as dt
import pandas as pd
from tqdm import tqdm
import ray
from data_model import db
from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, labels, daily_stats
from utilities import date_fu



def tick_sampler_workflow(config: dict, ray_on: bool=False) -> bool:    
    # find open market dates
    requested_open_dates = date_fu.get_open_market_dates(config['meta']['start_date'], config['meta']['end_date'])
    # avialiable tick backfill dates
    backfilled_dates = db.list_symbol_dates(symbol=config['meta']['symbol'], prefix='/data/trades')
    # requested+avialiable tick dates
    requested_backfilled_dates = list(set(backfilled_dates).intersection(requested_open_dates))
    # existing dates from results store
    existing_config_dates = db.list_symbol_dates(
        symbol=config['meta']['symbol'],
        prefix=f"/samples/{config['meta']['config_id']}/bar_date/"
        )
    # remaining, requested, aviable, dates
    final_remaining_dates = date_fu.find_remaining_dates(
        request_dates=requested_backfilled_dates,
        existing_dates=existing_config_dates,
        )
    # get daily stats for symbol (for dynamic sampling)
    daily_stats_df = daily_stats.get_symbol_stats(
        config['meta']['symbol'],
        config['meta']['start_date'],
        config['meta']['end_date'],
        source='local',
        )
    if ray_on:
        sample_date_ray = ray.remote(sample_date)

    bar_date = []
    for row in daily_stats_df.itertuples():
        if 'range_jma_lag' in daily_stats_df.columns:
            # update sampling renko_size based on recent daily range/volitility (and %value constraints)
            rs = max(row.range_jma_lag / config['sampler']['renko_range_frac'],
                    row.vwap_jma_lag * (config['sampler']['renko_range_min_pct_value'] / 100))  # force min
            rs = min(rs, row.vwap_jma_lag * 0.005)  # enforce max
            config['sampler'].update({'renko_size': rs})

        # sample ticks and store output in s3/b2
        if ray_on:
            bar_date = sample_date_ray.remote(config, row.date, save_output=True)
        else:
            bar_date = sample_date(config, row.date, save_output=True)

        bar_date.append(bar_date)

    if ray_on:
        ray.get(bar_date)  # wait until distrbuited work is finished

    return bar_date


def sample_date(config: dict, date: str, save_output: bool=False) -> dict:

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

    if save_output:
        presist_output(bar_date, date)

    return bar_date


def presist_output(bar_date: dict, date: str):
    # save bars_df
    db.write_df_to_fs(
        df=bar_date['bars_df'], 
        symbol=bar_date['config']['meta']['symbol'], 
        date=date, 
        prefix=f"/tick_samples/{bar_date['config']['meta']['config_id']}/bars_df/",
        )
    # drop dataframes
    del bar_date['bars_df']
    del bar_date['ticks_df']
    # save full results
    db.write_sdpickle(
        symbol=bar_date['config']['meta']['symbol'],
        date=date, 
        prefix=f"/tick_samples/{bar_date['config']['meta']['config_id']}/bar_date/",
        )
