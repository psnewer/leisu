# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from conf import *
from mean_odds_trenf import *
from asc_time_trenf import *
from bottom_trenf import *
from has_bottom_trenf import *
from bottom_rush_trenf import *
from mom_trenf import *
from first_down_trenf import *
from mom_rush_trenf import *
from long_mean_trenf import *
from round_rt_trenf import *
from abstract_trenf import *

class Trenf_Creator(object):
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
		if 'date' in df:
			df = df.groupby(['experiment_id','date'],as_index=False).agg(lambda x: tuple(x[x.notnull()])).applymap(lambda x: x[0] if type(x) is tuple and len(x)>0 else [] if x is () else x)
		else:
			df = df.groupby(['experiment_id'],as_index=False).agg(lambda x: tuple(x[x.notnull()])).applymap(lambda x: x[0] if type(x) is tuple and len(x)>0 else [] if x is () else x)
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

