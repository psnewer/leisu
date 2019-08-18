# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from abstract_trent import *
from conf import *

class Trent_Creator(object):
	def __init__(self,trend_creator,trenf_creator):
		self.trend_creator = trend_creator
		self.trenf_creator = trenf_creator
		_testerstr = 'ABSTRACT_TRENT'+'()'
		self.tester = eval(_testerstr)
	
	def setProcessor(self,features,filters):
		self.trend_creator.set_features(features)
		self.trenf_creator.set_filters(filters)

	def execute(self,dic_res,trend_log):
		team_res = self.trend_creator.execute_test(dic_res,trend_log)
		self.trenf_creator.execute(team_res)

	def predict(self,dic_res,trend_log):
		team_res = self.trend_creator.execute_predict(dic_res,trend_log)
		self.trenf_creator.execute(team_res)

	def get_filtered(self,filter_list):
		return self.trenf_creator.get_filtered(filter_list)

	def test(self,filters,dic_res,experiment_ids,test_id):
		df_filter = self.trenf_creator.get_filtered(filters)
		if df_filter is not None and len(df_filter) > 0:
			return self.tester.analysis(dic_res,df_filter,test_id,experiment_ids)
		else:
			return []

