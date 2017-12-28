#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import codecs
import pandas as pd
import numpy as np

class ABSTRACT_TRENT():
	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_TRENT'

	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

	def analysis(self,dic_res,df_team,test_id):
		team_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			for i in experiment_dic:
				if experiment_dic[i]:
					pre_detail = experiment_dic[i]
					df_pre = pd.DataFrame(pre_detail)
					df_pre['experiment_id'] = experiment_id
					df_pre['pre'] = i
					anadf = pd.merge(df_pre,df_team,how='inner',on=['experiment_id','pre','date'])
					if experiment_id==5 and i==2:
						anadf.to_csv('./log.csv')
					if len(anadf) == 0:
						continue
					per_count = anadf['res'].value_counts()
					succ = 0
					fail = 0
					neu = 0
					limi_odds = 100.0
					limi_odds_with_neu = 100.0
					if 0 in per_count:
						neu = per_count[0]
					if 1 in per_count:
						succ = per_count[1]
					if 2 in per_count:
						fail = per_count[2]
					if succ > 0:
						limi_odds = float(succ + fail)/float(succ)
						limi_odds_with_neu = float(succ + fail + neu)/float(succ)
					res = {}
					res['experiment_id'] = experiment_id
					res['test_id'] = test_id
					res['pre'] = i
					res['limi_odds'] = limi_odds
					res['limi_odds_with_neu'] = limi_odds_with_neu
					team_res.append(res)
		return team_res
