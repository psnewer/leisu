#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class HOME_WIN_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'HOME_WIN_FILTER'
		self.params['area'] = 1
		self.params['ignore'] = True

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if row['area'] != self.params['area'] and self.params['area'] > 0:
				if self.params['ignore']:
					continue
				else:
					delete_row.append(idx)
					continue
			if row['area'] == self.params['area']:
				to_team = row['toteam']
				date = row['date']
				row_to = df.query("team_id==%d & date=='%s'"%(to_team,date)).iloc[-1]
				weight_score0 = row['PRE_RANK_FEATURE'][0]['score'] - row_to['PRE_RANK_FEATURE'][0]['score']
				weight_num0 = row['PRE_RANK_FEATURE'][0]['number'] - row_to['PRE_RANK_FEATURE'][0]['number']
				if weight_num0 > 0 and row_to['PRE_RANK_FEATURE'][0]['number'] > 0:
					home_mean_score0 = float(row['PRE_RANK_FEATURE'][0]['score'])/row['PRE_RANK_FEATURE'][0]['number']
					home_away_score0 = float(row_to['PRE_RANK_FEATURE'][0]['score'])/row_to['PRE_RANK_FEATURE'][0]['number']
					weight_score0 = (home_mean_score0 - home_away_score0)* row_to['PRE_RANK_FEATURE'][0]['number']
				if weight_score0 > 0:
					min_length = min(2,len(row['PRE_RANK_FEATURE']))
					for i in range(1,min_length):
						if row['PRE_RANK_FEATURE'][i]!={} and row_to['PRE_RANK_FEATURE'][i]!={}:
							if row['PRE_RANK_FEATURE'][i]['score'] > row_to['PRE_RANK_FEATURE'][i]['score']:
								continue
			delete_row.append(idx)
		df.drop(delete_row,inplace=True)
		return df
