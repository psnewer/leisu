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
		f_res = open(gflags.FLAGS.res_path,'w+')
		f_res.close()

	def set_features(self,feature_list):
		self.feature_cand = []
		for feature in feature_list:
			self.feature_cand.append(feature)

	def execute(self,condition,action):
		team_res = []
		for feature in self.feature_cand:
			feature_ins = self.features[feature]
			res = feature_ins.execute(condition,action)
			team_res.extend(res)
		return team_res

