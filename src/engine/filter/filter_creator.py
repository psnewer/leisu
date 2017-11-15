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
from vs_plainself_filter import *
from counter_vs_filter import *

class Filter_Creator(object):
	def __init__(self,str_filters):
		self.filters = dict()
		self.filter_cand = []
		for filter in str_filters:
			filterstr = filter['name']+'()'
			filter_ins = eval(filterstr)
			filter_ins.setParams(filter['params'])
			self.filters[filter['flag']] = filter_ins
			self.filter_cand.append(filter['flag'])
		self.filter_res = {}
		self.df = None
	
	def set_filters(self,filter_list):
		self.filter_cand = []
		for filter in filter_list:
			self.filter_cand.append(filter)

	def execute(self,feature_list):
		if len(feature_list) == 0:
			self.df = None
			for filter in self.filter_cand:
				self.filter_res[filter] = []
			return
		df = pd.DataFrame(feature_list)
		df = df.groupby(['date','team_id','area'],as_index=False).agg(lambda x: x[x.notnull()].tail(1))
		self.df = df
		for filter in self.filter_cand:
			filter_ins = self.filters[filter]
			df_delist = filter_ins.filter(df)
			self.filter_res[filter] = df_delist

	def get_filtered(self,filter_list):
		if self.df is None:
			return
		delete_list = []
		for filter in filter_list:
			delete_list.extend(self.filter_res[filter])
		delete_list = list(set(delete_list))
		return self.df.drop(delete_list,inplace=False)

