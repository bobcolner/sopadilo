import datetime as dt
import numpy as np
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from filters import mad, jma, tick_rule


class StreamingTickFilter:

    def __init__(self, thresh: dict):
        self.irregular_conditions = [2, 5, 7, 10, 13, 15, 16, 20, 21, 22, 29, 33, 38, 52, 53]
        self.mad_filter = mad.MADFilter(
            value_winlen=thresh['mad_value_winlen'], 
            deviation_winlen=thresh['mad_deviation_winlen'], 
            k=thresh['mad_k']
            )
        self.jma_filter = jma.JMAFilter(winlen=thresh['jma_winlen'], power=thresh['jma_power'])
        self.tick_rule = tick_rule.TickRule()
        self.update_counter = 0
        self.ticks = []


    def update(self, price: float, volume: int, sip_dt: Timestamp, exchange_dt: Timestamp, conditions: np.ndarray) -> tuple:

        self.update_counter += 1        
        self.mad_filter.update(next_value=price)  # update mad filter
        tick = {
            'price': price,
            'volume': volume,
            'nyc_dt': sip_dt.tz_localize('UTC').tz_convert('America/New_York'),
            'status': 'raw',
            'mad': self.mad_filter.mad[-1],
            }

        if volume < 1:  # zero volume/size tick
            tick['status'] = 'filtered: zero volume'
        elif pd.Series(conditions).isin(self.irregular_conditions).any():  # 'irrgular' tick condition
            tick['status'] = 'filtered: irregular condition'
        elif abs(sip_dt - exchange_dt) > pd.to_timedelta(3, unit='S'):  # large ts deltas
            tick['status'] = 'filtered: ts diff'
        elif self.mad_filter.status == 'mad_warmup':
            tick['status'] = 'filtered: mad_warmup'
        elif self.mad_filter.status ==  'mad_outlier':
            tick['status'] = 'filtered: mad_outlier'
        else:  # 'clean' tick
            # update jma filter
            tick['price_jma'] = self.jma_filter.update(next_value=price)
            # update tick rule 'side'
            tick['side'] = self.tick_rule.update(next_price=price)  # update tick rule
            # mark pre/post/open market hours
            if tick['nyc_dt'].to_pydatetime().time() < dt.time(hour=9, minute=30):
                tick['status'] = 'clean: pre-market'
            elif tick['nyc_dt'].to_pydatetime().time() >= dt.time(hour=16, minute=00):
                tick['status'] = 'clean: after-hours'
            else:
                tick['status'] = 'clean: market-open'

        self.ticks.append(tick)


    def batch_update(self, ticks_df: pd.DataFrame):
        for tick in ticks_df.itertuples():
            self.update(sip_dt=tick.sip_dt, exchange_dt=tick.exchange_dt,
                price=tick.price, volume=tick.volume, conditions=tick.conditions)
