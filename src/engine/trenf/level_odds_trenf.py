#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class LEVEL_ODDS_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'LEVEL_ODDS_TRENF'
		self.params = {}
		self.params['up_scale'] = 0.25
		self.params['down_scale'] = 0.2
		self.params['up'] = True
		self.params['filt'] = True

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'MEAN_ODDS_TREND' in row and 'CURRENT_ODDS_TREND' in row:
				if 1 in row['CURRENT_ODDS_TREND'] and row['MEAN_ODDS_TREND']:
					odds = row['CURRENT_ODDS_TREND'][1]['all']
					mean_odds = row['MEAN_ODDS_TREND']
					if self.params['up']:
						if not (odds > mean_odds and odds < mean_odds*(1.0+self.params['up_scale'])):
							if self.params['filt']:
								continue
						else:
							if not self.params['filt']:
								continue
					else:
						if not (odds < mean_odds*(1.0-self.params['down_scale']) and odds > 0.0):
							if self.params['filt']:
								continue
						else:
							if not self.params['filt']:
								continue
			delete_row.append(idx)
		return delete_row
