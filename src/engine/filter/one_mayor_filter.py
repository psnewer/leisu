#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class ONE_MAYOR_FILTER(ABSTRACT_FILTER):

	def __init__(self):
		self.name = 'ONE_MAYOR_FILTER'
		self.params = {}
		self.params['tar'] = 1

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'ONE_MAYOR_FEATURE' in row:
				if row['ONE_MAYOR_FEATURE'] == self.params['tar']:
					continue
			delete_row.append(idx)
		return delete_row
