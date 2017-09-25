# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from conf import *
from one_mayor_filter import *
from two_mayor_filter import *
from minor_period_filter import *
from current_score_filter import *
from home_win_filter import *
from min_team_filter import *
from vs_filter import *

class Filter_Creator(object):
	def __init__(self,str_filters):
		self.filters = dict()
		self.filter_cand = []
		for filter in str_filters:
			filterstr = filter['name']+'()'
			filter_ins = eval(filterstr)
			self.filters[filter['name']] = filter_ins
	
	def set_filters(self,filter_list):
		self.filter_cand = []
		for filter in filter_list:
			self.filter_cand.append(filter['name'])
			self.filters[filter['name']].setParams(filter['params'])

	def execute(self,feature_list):
		df = pd.DataFrame(feature_list)
		df = df.groupby(['date','team_id'],as_index=False).agg(lambda x: x[x.notnull()].tail(1))
		for filter in self.filter_cand:
			filter_ins = self.filters[filter]
			df = filter_ins.filter(df)
		return df

						
