#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_filter import *

class HOME_WIN_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'HOME_WIN_FILTER'

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'RANK_STATUS_FEATURE' in row:
				if row['HOME_WIN_FEATURE']['mayorstatus'] == self.params['tar']:
					continue
			delete_row.append(idx)
		df.drop(delete_row,inplace=True)
		return df
