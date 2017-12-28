#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_trenf import *

class MEAN_ODDS_TRENF(ABSTRACT_TRENF):

	def __init__(self):
		self.name = 'MEAN_ODDS_TRENF'
		self.params = {}
		self.params['mean_thresh'] = 2.2

	def filter(self,df):
		delete_row = []
		for idx,row in df.iterrows():
			if 'MEAN_ODDS_TREND' in row and row['MEAN_ODDS_TREND']:
				if row['MEAN_ODDS_TREND'] < self.params['mean_thresh']:
					continue
			delete_row.append(idx)
		return delete_row
