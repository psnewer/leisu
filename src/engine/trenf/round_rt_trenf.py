#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class ROUND_RT_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'ROUND_RT_TRENF'
		self.params = {}
		self.params['thresh_rt'] = 0.9

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'ROUND_RT_TREND' in row and 'rt' in row['ROUND_RT_TREND']:
				rt = row['ROUND_RT_TREND']['rt']
				if rt < self.params['thresh_rt']:
					continue
			delete_row.append(idx)
		return delete_row
