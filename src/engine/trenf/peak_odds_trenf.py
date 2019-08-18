#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class PEAK_ODDS_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'PEAK_ODDS_TRENF'
		self.params = {}
		self.params['with_neu'] = False

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'PEAK_ODDS_TREND' in row and 'CURRENT_ODDS_TREND' in row:
				if self.params['with_neu']:
					if 'max_peak' in row['PEAK_ODDS_TREND']['limi_odds_with_neu']:
						max_peak = row['PEAK_ODDS_TREND']['limi_odds_with_neu']['max_peak']
						if 1 in row['CURRENT_ODDS_TREND']['limi_odds_with_neu'] and row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][1]['all'] != 100.0:
							pre_odds = row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][1]['all']
							if pre_odds > max_peak:
								continue
							elif pre_odds == max_peak:
								if 2 in row['CURRENT_ODDS_TREND']['limi_odds_with_neu'] and pre_odds > row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][2]['all']:
									continue
				else:
					if 'max_peak' in row['PEAK_ODDS_TREND']['limi_odds']:
						max_peak = row['PEAK_ODDS_TREND']['limi_odds']['max_peak']
						if 1 in row['CURRENT_ODDS_TREND']['limi_odds'] and row['CURRENT_ODDS_TREND']['limi_odds'][1]['all'] != 100.0:
							pre_odds = row['CURRENT_ODDS_TREND']['limi_odds'][1]['all']
							if pre_odds > max_peak:
								continue
							elif pre_odds == max_peak:
								if 2 in row['CURRENT_ODDS_TREND']['limi_odds'] and pre_odds > row['CURRENT_ODDS_TREND']['limi_odds'][2]['all']:
									continue
			delete_row.append(idx)
		return delete_row
