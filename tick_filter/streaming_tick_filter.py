import datetime as dt
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from filters import mad, jma, tick_rule

import warnings
warnings.simplefilter('ignore')  # annoying nanosceond conversion warning

class StreamingTickFilter:

    def __init__(self, mad_value_winlen: int=22, mad_deviation_winlen: int=333, mad_k: int=11,
                jma_winlen: int=7, jma_power: int=1, ts_diff_limit_sec: int=3):

        self.irregular_conditions = [2, 5, 7, 10, 13, 15, 16, 20, 21, 22, 29, 33, 38, 52, 53]
        self.ts_diff_limit_sec = ts_diff_limit_sec
        self.mad_filter = mad.MADFilter(value_winlen=mad_value_winlen, deviation_winlen=mad_deviation_winlen, k=mad_k)
        self.jma_filter = jma.JMAFilter(winlen=jma_winlen, power=jma_power)
        self.tick_rule = tick_rule.TickRule()
        self.update_counter = 0
        self.ticks = []

    def update(self, price: float, volume: int, sip_dt: Timestamp, exchange_dt: Timestamp, conditions: list) -> tuple:

        self.update_counter += 1        
        self.mad_filter.update(next_value=price)  # update mad filter
        tick = {
            'price': price,
            'volume': volume,
            'nyc_dt': sip_dt.tz_localize('UTC').tz_convert('America/New_York'),
            'status': 'raw',
            'mad': self.mad_filter.mad[-1],
            }
        # if conditions is not None:
        #     conditions = conditions.copy()  # prevent 'ValueError: buffer source array is read-only'
        if volume < 1:  # zero volume/size tick
            tick['status'] = 'filtered: zero volume'
        elif pd.Series(conditions).isin(self.irregular_conditions).any():  # 'irrgular' tick condition
            tick['status'] = 'filtered: irregular condition'
        elif abs(sip_dt - exchange_dt) > pd.to_timedelta(self.ts_diff_limit_sec, unit='S'):  # large ts deltas
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
            self.update(
                price=tick.price,
                volume=tick.volume,
                sip_dt=tick.sip_dt,
                exchange_dt=tick.exchange_dt,
                conditions=tick.conditions,
            )
