#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class COUNTER_VS_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'COUNTER_VS_FILTER'
		self.params = {}
		self.params['area'] = 1
		self.params['ignore'] = True
		self.params['tolast'] = 3
		self.params['equal'] = True

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if row['area'] != self.params['area'] and self.params['area'] > 0:
				if self.params['ignore']:
					continue
				else:
					delete_row.append(idx)
					continue
			to_team = row['toteam']
			date = row['date']
			num_win = 0
			num_lose = 0
			num = 0
			if (row['PRE_VS_FEATURE'][0]):
				for i in range(0,len(row['PRE_VS_FEATURE'][0])):
					num += 1
					_dic = row['PRE_VS_FEATURE'][0][i]
					delta_score = _dic['home_goal'] - _dic['away_goal']
					if delta_score > 0:
						if (_dic['area'] == 1):
							num_win += 1
						else:
							num_lose += 1
					elif delta_score < 0:
						if (_dic['area'] == 1):
							num_lose += 1
						else:
							num_win += 1
			if num == 0:
				delete_row.append(idx)
				continue
			winrt0 = float(num_win)/float(num)
			num_win = 0
			num_lose = 0
			num = 0
			min_length = min(self.params['tolast'],len(row['PRE_VS_FEATURE']))
			for i in range(1,min_length):
				num += 1
				_dics = row['PRE_VS_FEATURE'][i]
				for j in range(0,len(_dics)):
					_dic = _dics[j]
					delta_score = _dic['home_goal'] - _dic['away_goal']
					if delta_score > 0:
						if (_dic['area'] == 1):
							num_win += 1
						else:
							num_lose += 1
					elif delta_score < 0:
						if (_dic['area'] == 1):
							num_lose += 1
						else:
							num_win += 1
			if num == 0:
				delete_row.append(idx)
				continue
			winrt_ = float(num_win)/float(num)
			if self.params['equal']:
				if winrt0 <= winrt_:
					continue
			else:
				if winrt0 < winrt_:
					continue
			delete_row.append(idx)
		return delete_row
