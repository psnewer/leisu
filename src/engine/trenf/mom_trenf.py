#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class MOM_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'MOM_TRENF'
		self.params = {}
		self.params['mean_thresh'] = 2.0
		self.params['00'] = 2
		self.params['01'] = 2
		self.params['02'] = 1
		self.params['10'] = 1
		self.params['11'] = 2
		self.params['12'] = 1
		self.params['cand'] = [1]

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row and 'MOM_TREND' in row and 'LONG_MEAN_TREND' in row and 'SHORT_MEAN_TREND' in row and 'ASC_TIME_TREND' in row:
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
				if mean_up > 0.0 and mean_down < 100.0 and mean_up != mean_down:
					cover = 0
					if row['SHORT_MEAN_TREND']['down'] > mean_up:
						cover = 2
					elif row['SHORT_MEAN_TREND']['up'] < mean_down:
						cover = 1
					delta_down = 100.0
					delta_up = 100.0
					asc = row['ASC_TIME_TREND']
					if 1 in row['MOM_TREND']['down'] and 2 in row['MOM_TREND']['down']:
						delta_down = row['MOM_TREND']['down'][2] - row['MOM_TREND']['down'][1]
					if 1 in row['MOM_TREND']['up'] and 2 in row['MOM_TREND']['up']:
						delta_up = row['MOM_TREND']['up'][2] - row['MOM_TREND']['up'][1]
					if 1 in self.params['cand'] and delta_up > 0.1 and delta_down > 0.1 and delta_down != 100.0 and delta_up != 100.0:
						if cover == 2:
							if asc >= self.params['12']:
								continue
						elif cover == 1:
							if asc >= self.params['11']:
								continue
						elif cover == 0:
							if asc >= self.params['10']:
								continue
					elif 2 in self.params['cand'] and ((delta_down >=0.0 and delta_down <= 0.1 and delta_up > 0.1 and delta_up!=100.0) or (delta_up >= 0.0 and delta_up <= 0.1 and delta_down > 0.1 and delta_down != 100.0)):
						if cover == 2:
							if asc >= self.params['02']:
								continue
						elif cover == 1:
							if asc >= self.params['01']:
								continue
						elif cover == 0:
							if asc >= self.params['00']:
								continue
					elif 3 in self.params['cand'] and (delta_down >=0.0 and delta_down <= 0.1 and delta_up > 0.1 and delta_up!=100.0):
							if cover == 2:
								if asc >= self.params['02']:
									continue
							elif cover == 1:
								if asc >= self.params['01']:
									continue
							elif cover == 0:
								if asc >= self.params['00']:
									continue
			delete_row.append(idx)
		return delete_row
