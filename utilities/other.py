import ray
from tick_sampler import daily_stats, meta


def sample_dates(thresh: dict) -> list:

    daily_stats_df = daily_stats.get_symbol_stats(thresh['meta']['symbol'], thresh['meta']['start_date'], thresh['meta']['end_date'])
    sample_date_ray = ray.remote(meta.sample_date)
    date_futures = []
    for row in daily_stats_df.itertuples():
        if 'range_jma_lag' in daily_stats_df.columns:
            rs = max(row.range_jma_lag / thresh['sampler']['renko_range_frac'],
                    row.vwap_jma_lag * (thresh['sampler']['renko_range_min_pct_value'] / 100))  # force min
            rs = min(rs, row.vwap_jma_lag * 0.005)  # enforce max
            thresh['sampler'].update({'renko_size': rs})

        bar_date = sample_date_ray.remote(thresh, row.date)
        date_futures.append(bar_date)

    bar_dates = ray.get(date_futures)

    return bar_dates


def curve_drop(distance_miles: float) -> float:
    # curnve drop in inches
    drop_inches = (distance_miles ** 2) * 8 / 12
    return drop_inches
