#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class HAS_BOTTOM_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'HAS_BOTTOM_TRENF'
		self.params = {}
		self.params['bottom'] = 1.4

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row:
				signal_bottom = False
				signal_peak = False
				for i in row['CURRENT_ODDS_TREND']:
					if row['CURRENT_ODDS_TREND'][i]['all'] >= 1.0 and row['CURRENT_ODDS_TREND'][i]['all'] < self.params['bottom']:
						signal_bottom = True
					elif row['CURRENT_ODDS_TREND'][i]['all'] > self.params['bottom'] and signal_bottom:
						signal_peak = True 
						break
				if not (signal_bottom and signal_peak):
					continue
			delete_row.append(idx)
		return delete_row
