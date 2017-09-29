# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from min_goals_tester import *
from may_goals_tester import *
from home_win_tester import *
from away_win_tester import *
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
		test_final = open(gflags.FLAGS.test_final,'w+')
		test_final.close()
		test_res = open(gflags.FLAGS.res_test,'w+')
		test_res.close()
	
	def set_tester(self,tester_list):
		self.tester_cand = []
		for tester in tester_list:
			self.tester_cand.append(tester)

	def execute(self,condition):
		self.process(condition)

	def process(self,condition):
		team_res = self.feature_creator.execute(condition,action='test')
		if team_res != []:
			self.filter_creator.execute(team_res)
	
	def test(self,filters,testers,condition):
		self.set_tester(testers)
		for tester in self.tester_cand:
			df_filter = self.filter_creator.get_filtered(filters)
			self.testers[tester].analysis(condition,df_filter)
						
