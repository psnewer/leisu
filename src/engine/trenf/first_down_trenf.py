#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class FIRST_DOWN_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'FIRST_DOWN_TRENF'
		self.params = {}
		self.params['mean_thresh'] = 2.0
		self.params['peak_up'] = 4.0
		self.params['rt'] = 0.25
		self.params['with_neu'] = False

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row and 'MOM_TREND' in row and 'LONG_MEAN_TREND' in row:
				if self.params['with_neu']:
					mean_odds = row['LONG_MEAN_TREND']['limi_odds_with_neu']
					mean_up = 0.0
					mean_down = 100.0
					for i in mean_odds:
						if mean_odds[i] > self.params['mean_thresh']:
							continue
						if mean_odds[i] > mean_up:
							mean_up = mean_odds[i]
						if mean_odds[i] < mean_down:
							mean_down = mean_odds[i]
					if mean_up > 0.0:
						if 1 not in row['MOM_TREND']['limi_odds_with_neu']['down']:
							first_peak = 0.0
							if 1 in row['MOM_TREND']['limi_odds_with_neu']['up']:
								first_peak = row['MOM_TREND']['limi_odds_with_neu']['up'][1]
							elif row['CURRENT_ODDS_TREND']['limi_odds_with_neu']:
								first_peak = row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][len(row['CURRENT_ODDS_TREND']['limi_odds_with_neu'])]['all']
							if first_peak >= mean_up*(1.0 + self.params['rt']) and first_peak <= self.params['peak_up']:
								pre_odds = row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][1]
								if pre_odds['all'] <= mean_up * (1.0 + self.params['rt']) and pre_odds['all'] > mean_up:
									continue
				else:
					mean_odds = row['LONG_MEAN_TREND']['limi_odds']
					mean_up = 0.0
					mean_down = 100.0
					for i in mean_odds:
						if mean_odds[i] > self.params['mean_thresh']:
							continue
						if mean_odds[i] > mean_up:
							mean_up = mean_odds[i]
						if mean_odds[i] < mean_down:
							mean_down = mean_odds[i]
					if mean_up > 0.0:
						if 1 not in row['MOM_TREND']['limi_odds']['down']:
							first_peak = 0.0
							if 1 in row['MOM_TREND']['limi_odds']['up']:
								first_peak = row['MOM_TREND']['limi_odds']['up'][1]
							elif row['CURRENT_ODDS_TREND']['limi_odds']:
								first_peak = row['CURRENT_ODDS_TREND']['limi_odds'][len(row['CURRENT_ODDS_TREND']['limi_odds'])]['all']
							if first_peak >= mean_up*(1.0 + self.params['rt']) and first_peak <= self.params['peak_up']:
								pre_odds = row['CURRENT_ODDS_TREND']['limi_odds'][1]
								if pre_odds['all'] <= mean_up * (1.0 + self.params['rt']) and pre_odds['all'] > mean_up:
									continue
			delete_row.append(idx)
		return delete_row
