#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class FIRST_PEAK_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'FIRST_PEAK_TRENF'
		self.params = {}

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row and 'PEAK_ODDS_TREND' in row:
				if 'first_peak' in row['PEAK_ODDS_TREND'] and 1 in row['CURRENT_ODDS_TREND']:
					first_peak = row['PEAK_ODDS_TREND']['first_peak']
					pre_odds = row['CURRENT_ODDS_TREND'][1]['all']
					if pre_odds >= first_peak and pre_odds != 100.0:
						num_asc = 0
						signal = True
						for i in range(2,len(row['CURRENT_ODDS_TREND'])+1):
							odds = row['CURRENT_ODDS_TREND'][i]['all']
							if odds == 100.0:
								break
							if odds > pre_odds:
								if num_asc > 0:
									signal = False
									break
							elif odds < pre_odds:
								num_asc += 1
							pre_odds = odds
						if signal:
							continue			
			delete_row.append(idx)
		return delete_row
