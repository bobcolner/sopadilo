import pandas as pd
import ray
from tick_sampler import daily_stats
from utilities import date_fu
from data_layer import storage_adaptor
from workflows import sampler_task
from utilities import globals_unsafe as g


def run(config: dict, ray_on: bool=False) -> list:
    daily_df = get_dates_from_config(config)
    print(len(daily_df), 'dates scheduled')
    return run_sampler_flow(config, daily_df, ray_on)


def get_dates_from_config(config: dict) -> pd.DataFrame:
    # find open market dates
    requested_open_dates = date_fu.get_open_market_dates(config['meta']['start_date'], config['meta']['end_date'])
    # all avialiable tick backfill dates
    db = storage_adaptor.StorageAdaptor(fs_type='s3_filecache', root_path=g.DATA_S3_PATH)
    backfilled_dates = db.list_symbol_dates(symbol=config['meta']['symbol'], prefix='/data/trades')
    # requested & avialiable dates
    requested_backfilled_dates = list(set(backfilled_dates).intersection(set(requested_open_dates)))
    # existing dates from results store
    existing_config_id_dates = db.list_symbol_dates(
        symbol=config['meta']['symbol'],
        prefix=f"/tick_samples/{config['meta']['config_id']}/bar_date/"
        )
    # remaining, requested, aviable, dates
    final_remaining_dates = list(set(requested_backfilled_dates).difference(set(existing_config_id_dates)))
    # get daily stats for symbol (for dynamic sampling)
    daily_stats_df = daily_stats.get_symbol_stats(
        config['meta']['symbol'],
        config['meta']['start_date'],
        config['meta']['end_date'],
        source='local',
        )

    return daily_stats_df


def run_sampler_flow(config: dict, daily_stats_df: pd.DataFrame, ray_on: bool=False):

    if ray_on:
        sample_date_ray = ray.remote(sampler_task.sample_date)

    results = []
    for row in daily_stats_df.itertuples():

        if 'range_jma_lag' in daily_stats_df.columns:
            # update sampling renko_size based on recent daily range/volitility (and %value constraints)
            rs = max(row.range_jma_lag / config['sampler']['renko_range_frac'],
                    row.vwap_jma_lag * (config['sampler']['renko_range_min_pct_value'] / 100))  # force min
            rs = min(rs, row.vwap_jma_lag * 0.005)  # enforce max
            config['sampler'].update({'renko_size': rs})

        # core distrbuited function: sample ticks and store output in s3/b2
        if ray_on:
            bar_date = sample_date_ray.remote(config, row.date, save_results_flag=True)
        else:
            bar_date = sampler_task.sample_date(config, row.date, save_results_flag=True)

        results.append(bar_date)

    if ray_on:
        results = ray.get(results)  # wait until distrbuited work is finished

    return results
