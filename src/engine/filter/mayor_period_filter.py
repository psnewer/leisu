#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class MAYOR_PERIOD_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'MAYOR_PERIOD_FILTER'
		self.params = {}
		self.params['min_length'] = 2
		self.params['period'] = 5
		self.params['mayrt'] = 0.6

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'CURRENT_MATCH_FEATURE' in row:
				current_score = row['CURRENT_MATCH_FEATURE']
				length = len(current_score)
				if length >= self.params['min_length']:
					num_mayor = self.analysis(current_score)
					_length = 0
					if(length < self.params['period']):
						_length = length
					else:
						_length = self.params['period']
					if (num_mayor >= self.params['mayrt']*_length):
						continue	
			delete_row.append(idx)
		return delete_row

	def analysis(self,current_score):
		mayor_num = 0
		for pre in current_score:
			if pre <= self.params['period']:
				if(int(current_score[pre]['home_goal']) + int(current_score[pre]['away_goal']) > 2.5):
					mayor_num = mayor_num + 1
		return mayor_num
