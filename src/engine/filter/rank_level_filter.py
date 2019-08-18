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
		self.params['thresh'] = 1
		self.params['area'] = 1
		self.params['ignore'] = True

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
				if row_to['PRE_RANK_FEATURE'][0]['number'] > 2:
					rank = float(row['PRE_RANK_FEATURE'][0]['score'])/row['PRE_RANK_FEATURE'][0]['number']
					rank_to = float(row_to['PRE_RANK_FEATURE'][0]['score'])/row_to['PRE_RANK_FEATURE'][0]['number']
					if (int(rank) - int(rank_to)) <= self.params['thresh']:
						continue
			else:
				delete_row.append(idx)	
			delete_row.append(idx)
		return delete_row
