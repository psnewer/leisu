#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class PEER_WIN_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'PEER_WIN_TRENF'
		self.params = {}
		self.params['tolast'] = 3
		self.params['with_neu'] = False

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row:
				if self.params['with_neu']
					num = 0
					for i in range (1,len(row['CURRENT_ODDS_TREND']['limi_odds_with_neu']) + 1):
						odds = row['CURRENT_ODDS_TREND']['limi_odds_with_neu'][i]['all']
						if odds == 1.0:
							num += 1
							continue
						else:
							break
					if num == self.params['tolast']:
						continue
				else:
					num = 0
					for i in range (1,len(row['CURRENT_ODDS_TREND']['limi_odds']) + 1):
						odds = row['CURRENT_ODDS_TREND']['limi_odds'][i]['all']
						if odds == 1.0:
							num += 1
							continue
						else:
							break
					if num == self.params['tolast']:
						continue
			delete_row.append(idx)
		return delete_row
