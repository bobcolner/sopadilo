import pandas as pd
import ray
from tick_sampler import daily_stats
from utilities import date_fu
from data_layer import data_access
from workflows import sampler_task


def run(config: dict, ray_on: bool=False) -> list:
    daily_df = get_dates_from_config(config)
    print(len(daily_df), 'dates scheduled')
    return run_sampler_flow(config, daily_df, ray_on)


def run_sampler_flow(config: dict, daily_stats_df: pd.DataFrame, ray_on: bool=False) -> list:

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

        # core distributed function: filter/sample ticks & presist output
        if ray_on:
            bar_date = sample_date_ray.remote(config, row.date, presist_flag=True, progress_bar=False)
        else:
            bar_date = sampler_task.sample_date(config, row.date, presist_flag=True, progress_bar=True)

        results.append(bar_date)

    if ray_on:
        results = ray.get(results)  # wait until distrbuited work is finished

    return results


def get_dates_from_config(config: dict) -> pd.DataFrame:

    # find open market dates
    requested_open_dates = date_fu.get_open_market_dates(config['meta']['start_date'], config['meta']['end_date'])
    # all avialiable tick backfill dates
    backfilled_dates = data_access.list(
        symbol=config['meta']['symbol'],
        prefix='/data/trades',
        source='remote',
        )
    # requested & avialiable dates
    requested_backfilled_dates = list(set(requested_open_dates).intersection(set(backfilled_dates)))

    # existing dates from results store
    existing_config_id_dates = data_access.list(
        symbol=config['meta']['symbol'],
        prefix=f"/tick_samples/{config['meta']['config_id']}/bar_date",
        source='remote',
        )
    # remaining, requested, aviable, dates
    final_remaining_dates = list(set(requested_backfilled_dates).difference(set(existing_config_id_dates)))

    # get daily stats for symbol (for dynamic sampling)
    daily_stats_df = daily_stats.get_symbol_stats(
        symbol=config['meta']['symbol'],
        start_date=config['meta']['start_date'],
        end_date=config['meta']['end_date'],
        source='local',
        )
    # return daily stats only for remaining dates
    if len(final_remaining_dates) > 0:
        return daily_stats_df.loc[daily_stats_df.date.isin(final_remaining_dates)]
    else:
        print('No remaining dates')
