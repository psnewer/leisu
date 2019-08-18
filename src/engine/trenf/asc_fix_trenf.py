#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class ASC_FIX_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'ASC_FIX_TRENF'
		self.params = {}
		self.params['time'] = 2
		self.params['thresh'] = 1.5
		self.params['with_neu'] = False

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row and 1 in row['CURRENT_ODDS_TREND']['limi_odds_with_neu']:
				if self.params['with_neu']:
					pre_odds = row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][1]['now']
					num_asc = 0
					for i in range(1,len(row['CURRENT_ODDS_TREND']['limi_odds_with_neu'])+1):
						odds = row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][i]['now']
						if odds == 100.0:
							break
						if odds >= self.params['thresh']:
							num_asc += 1
						elif odds < self.params['thresh']:
							break
					if num_asc >= self.params['time']:
						continue
				else:	
					pre_odds = row['CURRENT_ODDS_TREND']['limi_odds'][1]['now']
					num_asc = 0
					for i in range(1,len(row['CURRENT_ODDS_TREND']['limi_odds'])+1):
						odds = row['CURRENT_ODDS_TREND']['limi_odds'][i]['now']
						if odds == 100.0:
							break
						if odds >= self.params['thresh']:
							num_asc += 1
						elif odds < self.params['thresh']:
							break
					if num_asc >= self.params['time']:
						continue
			delete_row.append(idx)
		return delete_row
