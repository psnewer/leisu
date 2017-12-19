#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class CURRENT_SCORE_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'CURRENT_SCORE_FILTER'
		self.params = {}
		self.params['tolast'] = 3
		self.params['score'] = 3
		self.params['area'] = 1
		self.params['ignore'] = True
		self.params['GE'] = True
		self.params['equal'] = True

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if row['area'] != self.params['area'] and self.params['area'] > 0:
				if self.params['ignore']:
					continue
				else:
					delete_row.append(idx)
					continue
			matches = row['CURRENT_MATCH_FEATURE']
			score = 0
			num = 0
			min_length = min(self.params['tolast'],len(matches))
			for i in range(1,min_length+1):
				num += 1
				if (matches[i]['area']) == 1:
					if matches[i]['home_goal'] > matches[i]['away_goal']:
						score += 3
					elif matches[i]['home_goal'] < matches[i]['away_goal']:
						score += 0
					else:
						score += 1
				else:
					if matches[i]['home_goal'] > matches[i]['away_goal']:
						score += 0
					elif matches[i]['home_goal'] < matches[i]['away_goal']:
						score += 3
					else:
						score += 1
			if self.params['GE']:
				if self.params['equal']:
					if score >= self.params['score']:
						continue
				else:
					if score > self.params['score']:
						continue
			elif not self.params['GE'] and num == self.params['tolast']:
				if self.params['equal']:
					if score <= self.params['score']:
						continue
				else:
					if score < self.params['score']:
						continue
			if num > 0:
				mean_score = float(score)/num
				mean_std = float(self.params['score'])/self.params['tolast']
				if self.params['GE']:
					if self.params['equal']:
						if mean_score >= mean_std:
							continue
					else:
						if mean_score > mean_std:
							continue
			delete_row.append(idx)
		return delete_row
