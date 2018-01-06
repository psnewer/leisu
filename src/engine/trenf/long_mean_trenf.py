#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class LONG_MEAN_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'LONG_MEAN_TRENF'
		self.params = {}
		self.params['mean_thresh'] = 2.0
		self.params['rt'] = 0.5

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'LONG_MEAN_TREND' in row:
				num_extra = 0
				for i in row['LONG_MEAN_TREND']:
					if row['LONG_MEAN_TREND'][i] > self.params['mean_thresh']:
						num_extra += 1
				if num_extra < len(row['LONG_MEAN_TREND']) * self.params['rt']: 
					continue
			delete_row.append(idx)
		return delete_row
