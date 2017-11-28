# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from min_goals_tester import *
from may_goals_tester import *
from home_win_tester import *
from away_win_tester import *
from draw_tester import *
from conf import *

class Tester_Creator(object):
	def __init__(self,str_testers,feature_creator,filter_creator):
		self.testers = dict()
		self.tester_cand = []
		self.feature_creator = feature_creator
		self.filter_creator = filter_creator
		for tester in str_testers:
			_testerstr = tester['name']+'()'
			tester_ins = eval(_testerstr)
			tester_ins.setParams(tester['params'])
			self.testers[tester['flag']] = tester_ins
	
	def set_tester(self,tester_list):
		self.tester_cand = []
		for tester in tester_list:
			self.tester_cand.append(tester)

	def setProcessor(self,features,filters):
		self.feature_creator.set_features(features)
		self.filter_creator.set_filters(filters)

	def group(self,condition,feature_log):
		team_res = self.feature_creator.execute_test(condition,feature_log)
		self.filter_creator.execute(team_res)

	def predict(self,league_id,serryid,df,feature_log):
		team_res = self.feature_creator.execute_predict(league_id,serryid,df,feature_log)
		if team_res != []:
			self.filter_creator.execute(team_res)

	def get_filtered(self,filter_list):
		return self.filter_creator.get_filtered(filter_list)

	def test(self,filters,testers,condition,tester_log):
		self.set_tester(testers)
		for tester in self.tester_cand:
			df_filter = self.filter_creator.get_filtered(filters)
			if df_filter is not None and len(df_filter) > 0:
				self.testers[tester].analysis(condition,df_filter,tester_log)
						
