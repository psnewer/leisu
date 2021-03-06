#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class TWO_MAYOR_FILTER(ABSTRACT_FILTER):

	def __init__(self):
		self.name = 'TWO_MAYOR_FILTER'
		self.params = {}
		self.params['type'] = 'eq'
		self.params['tar'] = 1

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'TWO_MAYOR_FEATURE' in row and row['TWO_MAYOR_FEATURE'] >= 0:
				if self.params['type'] == 'eq':
					if row['TWO_MAYOR_FEATURE'] == self.params['tar']:
						continue
				elif self.params['type'] == 'l':
					if row['TWO_MAYOR_FEATURE'] < self.params['tar']:
						continue
				elif self.params['type'] == 'le':
					if row['TWO_MAYOR_FEATURE'] <= self.params['tar']:
						continue
				elif self.params['type'] == 'g':
					if row['TWO_MAYOR_FEATURE'] > self.params['tar']:
						continue
				elif self.params['type'] == 'ge':
					if row['TWO_MAYOR_FEATURE'] >= self.params['tar']:
						continue
			delete_row.append(idx)
		return delete_row
