#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class RANK_WIN_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'RANK_WIN_FILTER'
		self.params = {}
		self.params['rank_thresh'] = 2
		self.params['win_thresh'] = 1
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
			if row['PRE_RANK_FEATURE'][0]['number'] > 0:
				rank = float(row['PRE_RANK_FEATURE'][0]['score'])/row['PRE_RANK_FEATURE'][0]['number']
				if rank < self.params['rank_thresh']:
					wins = row['WIN_SCORE_FEATURE']
					if wins >= self.params['win_thresh']:
						delete_row.append(idx)
		return delete_row
