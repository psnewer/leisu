#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class BOTTOM_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'BOTTOM_TRENF'
		self.params = {}
		self.params['bottom'] = 1.4
		self.params['rt'] = 0.6
		self.params['with_neu'] = False

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'PEAK_ODDS_TREND' in row:
				if self.params['with_neu']:
					peaks = row['PEAK_ODDS_TREND']['limi_odds_with_neu']
					num_ebb = 0
					for i in peaks['min_odds']:
						if peaks['min_odds'][i] <= self.params['bottom']:
							num_ebb += 1
					if num_ebb > self.params['rt'] * len(peaks['min_odds']):
						continue
				else:
					peaks = row['PEAK_ODDS_TREND']['limi_odds']
					num_ebb = 0
					for i in peaks['min_odds']:
						if peaks['min_odds'][i] <= self.params['bottom']:
							num_ebb += 1
					if num_ebb > self.params['rt'] * len(peaks['min_odds']):
						continue
			delete_row.append(idx)
		return delete_row
