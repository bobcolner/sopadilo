import datetime as dt
import pandas as pd
from tqdm import tqdm
from data_model import arrow_dataset, s3_backend
from tick_filter import streaming_tick_filter
from tick_sampler import streaming_tick_sampler, labels, daily_stats


def tick_sampler_workflow(config: dict, start_date: str, end_date: str):

    # avialiable tick dates
    symbol_dates = s3_backend.list_symbol_dates(symbol=config['meta']['symbol'], tick_type='trades')
    symbol_dates_dt = pd.to_datetime(symbol_dates)
    # requested+avialiable tick dates
    req_avb_dt = pd.DataFrame(symbol_dates_dt[(symbol_dates_dt >= start_date) & (symbol_dates_dt <= end_date)])
    # requested+unavialiable dates
    req_unavb_dt = pd.DataFrame(symbol_dates_dt[(symbol_dates_dt < start_date) | (symbol_dates_dt > end_date)])
    # existing dates from results store
    existing_dates = s3_backend.ls('polygon-equities/data/samples/test1')
    # get unfinished 'remaining' dates from list of requested+avialiable dates
    remaining_dates = find_remaining_dates(request_dates=req_avb_dt, existing_dates=existing_dates)
    # get daily stats for symbol (for dynamic sampling)
    daily_stats_df = daily_stats.get_symbol_stats(config['meta']['symbol'], config['meta']['start_date'], config['meta']['end_date'])
    if ray_on:
        sample_date_ray = ray.remote(sample_date)
        date_futures = []

    for date in remaining_dates:
        if 'range_jma_lag' in daily_stats_df.columns:
            # update sampling renko_size based on recent daily range/volitility (and %value constraints)
            range_jma_lag = daily_stats_df.loc[daily_stats_df['date'] == date]['range_jma_lag'].values[0]
            vwap_jma_lag = daily_stats_df.loc[daily_stats_df['date'] == date]['vwap_jma_lag'].values[0]
            rs = max(range_jma_lag / config['sampler']['renko_range_frac'],
                    vwap_jma_lag * (config['sampler']['renko_range_min_pct_value'] / 100))  # force min
            rs = min(rs, vwap_jma_lag * 0.005)  # enforce max
            config['sampler'].update({'renko_size': rs})

        # sample ticks and store output in s3/b2
        if ray_on:
            bar_date = sample_date_ray.remote(config, date, save_output=True)
            date_futures.append(bar_date)
        else:
            sample_date(config, date, save_output=True)

    if ray_on:
        ray.get(date_futures)



def presist_output(bar_date: dict, config: dict):
    del bar_date['ticks_df']
    file_path_end = f"tick_samples/{config['meta']['config_id']}/symbol={config['meta']['symbol']}/date={date}/"
    put_pickle_to_s3(obj=bar_date, s3_path=file_path_end)


def sample_date(config: dict, date: str, save_output: bool=False):

    tick_filter = streaming_tick_filter.StreamingTickFilter(**config['filter'])
    tick_sampler = streaming_tick_sampler.StreamingTickSampler(config['sampler'])
    # get raw trades
    tdf = s3_backend.fetch_date_df(symbol=config['meta']['symbol'], date=date, tick_type='trades')
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
        presist_output(bar_date, config)

    return bar_date
