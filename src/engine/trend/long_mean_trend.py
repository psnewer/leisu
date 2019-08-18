#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
import copy
from abstract_trend import *

class LONG_MEAN_TREND(ABSTRACT_TREND):
	def __init__(self):
		self.name = 'LONG_MEAN_TREND'
		self.params = {}
		self.params['num_season'] = 3

	def execute_predict(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			if 0 in experiment_dic and experiment_dic[0]:
				res = {}
				df_current = pd.DataFrame(experiment_dic[0])	
				res[self.name] = self.process(experiment_dic,df_current,0)
				res['experiment_id'] = experiment_id
				date_res.append(res)
				res_str = json.dumps(res,cls=GenEncoder)
				trend_log.write(res_str+'\n')
		return date_res

	def execute_test(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			for i in experiment_dic:
				if experiment_dic[i]:
					df_current = pd.DataFrame(experiment_dic[i]).sort_values(by='date')
					for idx,row in df_current.iterrows():
						df_pre = df_current.head(idx)
						res = self.process(experiment_dic,df_pre,i)
						res_detail = {}
						date = row['date']
						res_detail[self.name] = res
						res_detail['date'] = date
						res_detail['experiment_id'] = experiment_id
						res_detail['pre'] = i
						date_res.append(res_detail)
						res_str = json.dumps(res_detail,cls=GenEncoder)
						trend_log.write(res_str+'\n')
		return date_res

	def process(self,experiment_dic,df_current,pre):
		mean_odds = {}
		mean_odds['limi_odds'] = {}
		mean_odds['limi_odds_with_neu'] = {}
		for i in range(pre,self.params['num_season'] + pre + 1):
			if i in experiment_dic and experiment_dic[i]:
				pre_detail = experiment_dic[i]
				if i == pre:
					df_team = df_current
				else:
					df_team = pd.DataFrame(pre_detail)
				per_count = df_team['res'].value_counts()
				succ = 0
				fail = 0
				neu = 0 
				if 0 in per_count:
					neu = per_count[0]
				if 1 in per_count:
					succ = per_count[1]
				if 2 in per_count:
					fail = per_count[2]
				if succ > 0:
					limi_odds =  float(succ + fail) / float(succ)
					limi_odds_with_neu = float(succ + fail + neu) / float(succ)
					mean_odds['limi_odds'][i-pre] = limi_odds
					mean_odds['limi_odds_with_neu'][i-pre] = limi_odds_with_neu
				else:
					mean_odds['limi_odds'][i-pre] = 100.0
					mean_odds['limi_odds_with_neu'][i-pre] = 100.0
		return mean_odds
