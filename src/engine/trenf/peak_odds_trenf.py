#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class PEAK_ODDS_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'PEAK_ODDS_TRENF'
		self.params = {}

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'PEAK_ODDS_TREND' in row and 'CURRENT_ODDS_TREND' in row:
				if 'max_peak' in row['PEAK_ODDS_TREND']:
					max_peak = row['PEAK_ODDS_TREND']['max_peak']
					if 1 in row['CURRENT_ODDS_TREND'] and row['CURRENT_ODDS_TREND'][1]['all'] != 100.0:
						pre_odds = row['CURRENT_ODDS_TREND'][1]['all']
						if pre_odds > max_peak:
							continue
						elif pre_odds == max_peak:
							if 2 in row['CURRENT_ODDS_TREND'] and pre_odds > row['CURRENT_ODDS_TREND'][2]['all']:
								continue
			delete_row.append(idx)
		return delete_row
