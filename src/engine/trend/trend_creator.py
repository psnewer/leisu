# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from mean_odds_trend import *
from current_odds_trend import *
from peak_odds_trend import *
from round_rt_trend import *
from conf import *

class Trend_Creator(object):
	def __init__(self,algs):
		self.features = dict()
		self.feature_cand = []
		for _alg in algs:
			_algstr = _alg['name']+'()'
			alg_ins = eval(_algstr)
			alg_ins.setParams(_alg['params'])
			self.features[_alg['flag']] = alg_ins
			self.feature_cand.append(_alg['flag'])
		self.feature_res = {}

	def set_features(self,feature_list):
		self.feature_cand = []
		for feature in feature_list:
			self.feature_cand.append(feature)

	def execute_test(self,condition,feature_log):
		team_res = []
		for feature in self.feature_cand:
			feature_ins = self.features[feature]
			res = feature_ins.execute_test(condition,feature_log)
			team_res.extend(res)
		return team_res

	def execute_predict(self,league_id,serryid,df,feature_log):
		team_res = []
		for feature in self.feature_cand:
			feature_ins = self.features[feature]
			res = feature_ins.execute_predict(league_id,serryid,df,feature_log)
			team_res.extend(res)
		return team_res
