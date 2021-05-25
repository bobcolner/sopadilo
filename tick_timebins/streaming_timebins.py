import datetime as dt
import pandas as pd
from utilities import stats


class StreamingTickBatcher:

	def __init__(self, start_time: str='09:30:00', end_time: str:='16:00:00', freq_sec: int=2):
		self.freq_sec = freq_sec
		self.current_sec = 0
		self.prevous_sec = 0
		self.ticks = []
		self.buckets = []

	def update(self, close_at: Timestamp, price: float, volume: int, side: int, price_jma: float):
		
		self.ticks.append(close_at)

		self.current_sec = close_at.second
		if len(self.ticks) > 0:
			if self.current_sec <> self.prevous_sec:
