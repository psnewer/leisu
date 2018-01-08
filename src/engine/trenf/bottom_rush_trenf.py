#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class BOTTOM_RUSH_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'BOTTOM_RUSH_TRENF'
		self.params = {}
		self.params['bottom'] = 1.4
		self.params['cand'] = 1

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_ODDS_TREND' in row and 1 in row['CURRENT_ODDS_TREND'] and row['CURRENT_ODDS_TREND'][1]['all'] > self.params['bottom'] and row['CURRENT_ODDS_TREND'][1]['all'] < 100.0:
				if 'MOM_TREND' in row and self.params['cand'] <= 4:
					delta_down = 0.0
					delta_up = 0.0
					if 1 in row['MOM_TREND']['down'] and 2 in row['MOM_TREND']['down']:
						delta_down = row['MOM_TREND']['down'][2] - row['MOM_TREND']['down'][1]
					if 1 in row['MOM_TREND']['up'] and 2 in row['MOM_TREND']['up']:
						delta_up = row['MOM_TREND']['up'][2] - row['MOM_TREND']['up'][1]
					if self.params['cand'] == 1:
						if delta_down > 0.0 or delta_up > 0.0:
							continue
					elif self.params['cand'] == 2:
						if delta_down > 0.0 and delta_up > 0.0:
							continue
					elif self.params['cand'] == 3:
						if delta_down > 0.1 or delta_up > 0.1:
							continue
					elif self.params['cand'] == 4:
							continue
				if 'ASC_TIME_TREND' in row and self.params['cand'] == 5:
					if row['ASC_TIME_TREND'] < -1:
						continue
			delete_row.append(idx)
		return delete_row	
