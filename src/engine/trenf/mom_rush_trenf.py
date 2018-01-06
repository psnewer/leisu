#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class MOM_RUSH_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'MOM_RUSH_TRENF'
		self.params = {}
		self.params['mean_thresh'] = 2.0
		self.params['rt'] = 0.25

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row and 'MOM_TREND' in row and 'LONG_MEAN_TREND' in row and 'SHORT_MEAN_TREND' in row:
				mean_odds = row['LONG_MEAN_TREND']
				mean_up = 0.0
				mean_down = 100.0
				for i in mean_odds:
					if mean_odds[i] > self.params['mean_thresh']:
						continue
					if mean_odds[i] > mean_up:
						mean_up = mean_odds[i]
					if mean_odds[i] < mean_down:
						mean_down = mean_odds[i]
				if mean_up > 0.0 and mean_down < 100.0:
					if 1 in row['MOM_TREND']['up'] and 2 in row['MOM_TREND']['up']:
						delta_up = row['MOM_TREND']['up'][2] - row['MOM_TREND']['up'][1]
						delta_down = 0.0
						if 1 in row['MOM_TREND']['down'] and 2 in row['MOM_TREND']['down']:
							delta_down = row['MOM_TREND']['down'][2] - row['MOM_TREND']['down'][1] 
						if delta_up > 0.1:
							if delta_down > 0.1:
								if row['SHORT_MEAN_TREND']['down'] >= mean_up:
									pre_odds = row['CURRENT_ODDS_TREND'][1]
									if pre_odds['all'] <= (1.0+self.params['rt'])*mean_up and pre_odds['all'] > mean_up:
										continue
			delete_row.append(idx)
		return delete_row
