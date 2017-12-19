# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from minor_period_feature import *
from one_mayor_feature import *
from two_mayor_feature import *
from pre_rank_feature import *
from pre_vs_feature import *
from min_team_feature import *
from current_match_feature import *
from win_score_feature import *
from vs_plainself_feature import *
from vs_plainself_no_pre_feature import *
from vs_rawall_feature import *
from vs_rawall_no_pre_feature import *
from goal_rawall_feature import *
from conf import *

class Feature_Creator(object):
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
