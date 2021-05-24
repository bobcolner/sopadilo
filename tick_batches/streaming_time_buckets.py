import datetime as dt
import pandas as pd
from utilities import stats


class StreamingTickBatcher:

	def __init__(self, freq: str='2s'):
		self.freq = freq
		self.buckets = []
		self.current_sec = 0

	def update(self, close_at: Timestamp, price: float, volume: int, side: int, price_jma: float):
		
		# self.current_sec = close_at.second
		pass
