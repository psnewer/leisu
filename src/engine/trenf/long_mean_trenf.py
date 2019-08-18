#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class LONG_MEAN_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'LONG_MEAN_TRENF'
		self.params = {}
		self.params['num_mean'] = 2
		self.params['with_neu'] = False

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'LONG_MEAN_TREND' in row:
				if self.params['with_neu']:
					std_mean = 100.0
					num_mean = 0
					for  i in row['LONG_MEAN_TREND']['limi_odds_with_neu']:
						if i != 0:
							if std_mean >= row['LONG_MEAN_TREND']['limi_odds_with_neu'][i]:
								std_mean = row['LONG_MEAN_TREND']['limi_odds_with_neu'][i]
							num_mean = num_mean + 1
							if num_mean > self.params['num_mean']:
								break
					current_mean = row['LONG_MEAN_TREND']['limi_odds_with_neu'][0]
					if current_mean >= std_mean:
						continue
				else:
					std_mean = 100.0
					num_mean = 0
					for i in row['LONG_MEAN_TREND']['limi_odds']:
						if i != 0:
							if std_mean >= row['LONG_MEAN_TREND']['limi_odds'][i]:
								std_mean = row['LONG_MEAN_TREND']['limi_odds'][i]
							num_mean = num_mean + 1
							if num_mean > self.params['num_mean']:
								break
					current_mean = row['LONG_MEAN_TREND']['limi_odds'][0]
					if current_mean >= std_mean:
						continue
			delete_row.append(idx)
		return delete_row
