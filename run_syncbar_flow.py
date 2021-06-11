import ray
from workflows import syncbar_flow


config = {
    'config_id': 'renko_v2',
    'start_date': '2019-01-01',
    'end_date': '2021-01-01',
    'source': 'remote',
    'destination': 'both',
    'on_ray': True,
}

ray.init(dashboard_host='0.0.0.0', dashboard_port=1111, ignore_reinit_error=True)

bds = syncbar_flow.run(config)

ray.shutdown()
