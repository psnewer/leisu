#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class RANK_LEVEL_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'RANK_LEVEL_FILTER'
		self.params = {}
		self.params['1_thresh'] = 2
		self.params['3_thresh'] = 0.85
		self.params['area'] = 1
		self.params['ignore'] = True
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
			if row['PRE_RANK_FEATURE'][0]['number'] > 2:
				to_team = row['toteam']
				date = row['date']
				row_to = df.query("team_id==%d & date=='%s'"%(to_team,date)).iloc[-1]
				if row_to['PRE_RANK_FEATURE'][0]['number'] > 0:
					rank = float(row['PRE_RANK_FEATURE'][0]['score'])/row['PRE_RANK_FEATURE'][0]['number']
					rank_to = float(row_to['PRE_RANK_FEATURE'][0]['score'])/row_to['PRE_RANK_FEATURE'][0]['number']
					if (rank >= self.params['1_thresh'] and rank_to < self.params['1_thresh']) or (rank_to >= self.params['1_thresh'] and rank < self.params['1_thresh']):
						delete_row.append(idx)	
					elif rank < self.params['1_thresh'] and rank > self.params['3_thresh'] and rank_to <= self.params['3_thresh']:
						delete_row.append(idx)
			else:
				delete_row.append(idx)	
		return delete_row
