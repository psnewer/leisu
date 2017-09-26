#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class MIN_TEAM_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'MIN_TEAM_FILTER'
		self.params = {}
		self.params['tolast'] = 2
		self.params['yes'] = 1
		self.params['no'] = 2

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			num_yes = 0
			num_no = 0
			min_length = min(self.params['tolast'],len(row['MIN_TEAM_FEATURE']))
			for i in range(0,min_length):
				if row['MIN_TEAM_FEATURE'][i] == self.params['yes']:
					num_yes += 1
				elif row['MIN_TEAM_FEATURE'][i] == self.params['no']:
					num_no += 1
			if (num_yes > 0 and num_yes > num_no):
				continue
			delete_row.append(idx)
		df.drop(delete_row,inplace=True)
		return df
