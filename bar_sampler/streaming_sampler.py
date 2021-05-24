import numpy as np
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from filter import mad, jma, tick_rule
from bar_sampler import sampler


class TickFilterBarSampler:

    def __init__(self, thresh: dict):
        self.irregular_conditions = [2, 5, 7, 10, 13, 15, 16, 20, 21, 22, 29, 33, 38, 52, 53]
        self.mad_filter = mad.MADFilter(
            value_winlen=thresh['mad_value_winlen'], 
            deviation_winlen=thresh['mad_deviation_winlen'], 
            k=thresh['mad_k']
            )
        self.jma_filter = jma.JMAFilter(winlen=thresh['jma_winlen'], power=thresh['jma_power'])
        self.tick_rule = tick_rule.TickRule()
        self.bar_sampler = sampler.BarSampler(thresh)
        self.ticks = []
        self.bars = []

    def update(self, price: float, volume: int, sip_dt: Timestamp, exchange_dt: Timestamp, conditions: np.ndarray) -> tuple:

        tick = {
            'price': price,
            'volume': volume,
            'nyc_dt': sip_dt.tz_localize('UTC').tz_convert('America/New_York'),
            'status': 'raw',
            }
        
        new_bar = {'bar_trigger': 'waiting'}

        if volume < 1:  # zero volume/size tick
            tick['status'] = 'filtered: zero volume'
        elif pd.Series(conditions).isin(self.irregular_conditions).any():  # 'irrgular' tick condition
            tick['status'] = 'filtered: irregular condition'
        elif abs(sip_dt - exchange_dt) > pd.to_timedelta(3, unit='S'):  # large ts deltas
            tick['status'] = 'filtered: ts diff'

        self.mad_filter.update(next_value=price)  # update mad filter

        if self.mad_filter.status == 'mad_warmup'
            tick['status'] = 'filtered: mad_warmup'
        elif self.mad_filter.status ==  'mad_outlier'
            tick['status'] = 'filtered: mad_outlier'
        else:  # 'clean' tick
            tick['status'] = 'clean'
            tick['jma'] = self.jma_filter.update(next_value=price)  # update jma filter
            tick['side'] = self.tick_rule.update(next_price=price)  # update tick rule
            if tick['nyc_dt'].to_pydatetime().time() < time(hour=9, minute=30):
                tick['status'] = 'clean_pre_market'
            elif tick['nyc_dt'].to_pydatetime().time() >= time(hour=16, minute=50):
                tick['status'] = 'clean_after_hours'
            else:
                tick['status'] = 'clean'
                self.bar_sampler.update(tick)

        self.ticks.append(tick)

        return tick, self.bar_sampler.bars
