#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class ASC_TIME_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'ASC_TIME_TRENF'
		self.params = {}
		self.params['type'] = 'eq'
		self.params['time'] = 1

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'ASC_TIME_TREND' in row:
				num = row['ASC_TIME_TREND']
				if self.params['type'] == 'eq':
					if num == self.params['time']:
						continue
				elif self.params['type'] == 'g':
					if num > self.params['time']:
						continue
				elif self.params['type'] == 'l':
					if num < self.params['time']:
						continue
				elif self.params['type'] == 'ge':
					if num >= self.params['time']:
						continue
				elif self.params['type'] == 'le':
					if num <= self.params['time']:
						continue
			delete_row.append(idx)
		return delete_row
