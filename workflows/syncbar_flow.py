import ray
from utilities import date_fu
from data_layer import data_access
from workflows import sampler_task


def run(config: dict):

    all_symbols = data_access.list_sd_data(prefix=f"/bars/{config['config_id']}/df", source=config['source'])
    large_caps, mid_caps = sampler_task.segment_symbols(all_symbols, start_date=config['start_date'], end_date=config['end_date'])
    if config['on_ray']:
        sync_symbol_date_ray = ray.remote(sampler_task.sync_symbol_date)
        futures = []

    for sym_midcap in mid_caps:
        for sym_largecap in large_caps:
            if sym_largecap == sym_midcap:
                continue

            dates = date_fu.query_dates(
                symbol=sym_largecap,
                prefix = f"/bars/{config['config_id']}/sync_bars/clock_symbol={sym_midcap}",
                start_date=config['start_date'],
                end_date=config['end_date'],
                query_type='remaining',
                source=config['source'],
            )
            print('syncing symbols:', sym_midcap, sym_largecap, 'scheduled', len(dates), 'dates')  # logging
            for date in dates:
                if config['on_ray']:
                    future = sync_symbol_date_ray.remote(
                        clock_symbol=sym_midcap,
                        sync_symbol=sym_largecap,
                        date=date,
                        config=config,
                        )
                    futures.append(future)
                else:
                    sampler_task.sync_symbol_date(
                        clock_symbol=sym_midcap, 
                        sync_symbol=sym_largecap, 
                        date=date, 
                        config=config
                        )
    if config['on_ray']:
        ray.get(futures)
