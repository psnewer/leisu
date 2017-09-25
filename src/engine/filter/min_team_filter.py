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
		self.params['tar'] = 1

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'MIN_TEAM_FEATURE' in row:
				if row['MIN_TEAM_FEATURE']['mayorstatus'] == self.params['tar']:
					continue
			delete_row.append(idx)
		df.drop(delete_row,inplace=True)
		return df
