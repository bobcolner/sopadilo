import ray
from workflows import sampler_flow
from workflows.configs import renko_v1, renko_v2, renko_v3


config = renko_v3.config

prefix_data = '/data/trades'
prefix_meta = f"/bars/{config['meta']['config_id']}/meta"
prefix_df = f"/bars/{config['meta']['config_id']}/df"

ray.init(dashboard_host='0.0.0.0', dashboard_port=1111, ignore_reinit_error=True)

bds = sampler_flow.run(config)

ray.shutdown()
