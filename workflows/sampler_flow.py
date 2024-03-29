import pandas as pd
import ray
from tick_sampler import daily_stats
from utilities import date_fu
from data_layer import data_access
from workflows import sampler_task


def run(config: dict) -> list:
    bar_data = []
    if (config['meta'].get('symbol_list')) and (len(config['meta']['symbol_list']) > 0):
        for symbol in config['meta']['symbol_list']:
            try:
                config['meta'].update({'symbol': symbol})
                bar_data = run_sampler_flow(config)
            except:
                print('failed symbol list:', config['meta']['symbol'])
                pass
    else:
        bar_data = run_sampler_flow(config)

    return bar_data


def run_sampler_flow(config: dict) -> list:

    daily_stats_df = get_dates_from_config(config)

    if config['meta']['on_ray']:
        sample_date_ray = ray.remote(sampler_task.sample_date)

    results = []
    for row in daily_stats_df.itertuples():
        try:
            if 'range_jma_lag' in daily_stats_df.columns:
                # update sampling renko_size based on recent daily range/volitility (and %value constraints)
                rs = max(row.range_jma_lag / config['sampler']['renko_range_frac'],
                        row.vwap_jma_lag * (config['sampler']['renko_range_min_pct_value'] / 100))  # force min
                rs = min(rs, row.vwap_jma_lag * 0.005)  # enforce max
                config['sampler'].update({'renko_size': rs})

            # core distributed function: filter/sample ticks & presist output
            if config['meta']['on_ray']:
                bar_date = sample_date_ray.remote(config, row.date, progress_bar=False)
            else:
                bar_date = sampler_task.sample_date(config, row.date, progress_bar=True)

            results.append(bar_date)
        except:
            print('failed on symbol date:', config['meta']['symbol'], row.date)
            pass

    if config['meta']['on_ray']:
        results = ray.get(results)  # wait until distrbuited work is finished

    return results


def get_dates_from_config(config: dict) -> pd.DataFrame:

    # find open market dates
    requested_open_dates = date_fu.get_open_market_dates(config['meta']['start_date'], config['meta']['end_date'])
    # all avialiable tick backfill dates
    backfilled_dates = data_access.list_sd_data(
        prefix='/data/trades',
        symbol=config['meta']['symbol'],
        source='remote',
        )
    # requested & avialiable dates
    requested_backfilled_dates = list(set(requested_open_dates).intersection(set(backfilled_dates)))

    # existing dates from results store
    existing_config_id_dates = data_access.list_sd_data(
        prefix=f"/bars/{config['meta']['config_id']}/meta",
        symbol=config['meta']['symbol'],
        source='remote',
        )
    # remaining, requested, aviable, dates
    final_remaining_dates = list(set(requested_backfilled_dates).difference(set(existing_config_id_dates)))

    # return daily stats only for remaining dates
    if len(final_remaining_dates) > 0:
        # get symbol daily stats for dynamic sampling
        daily_stats_df = daily_stats.get_symbol_stats(
            symbol=config['meta']['symbol'],
            start_date=config['meta']['start_date'],
            end_date=config['meta']['end_date'],
            )
        return daily_stats_df.loc[daily_stats_df.date.isin(final_remaining_dates)]
    else:
        raise Exception("No remaining dates")
