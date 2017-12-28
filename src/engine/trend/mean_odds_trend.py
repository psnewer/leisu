#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
import copy
from abstract_trend import *

class MEAN_ODDS_TREND(ABSTRACT_TREND):
	def __init__(self):
		self.name = 'MEAN_ODDS_TREND'
		self.params = {}
		self.params['with_neu'] = False
		self.params['num_season'] = 3
		self.params['max_mean'] = 2.5
		self.params['max_rt'] = 0.5
		self.params['mean_peak'] = 2.2

	def execute_predict(self,league_id,serryid,df_serry,feature_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			res[self.name] = self.process(experiment_dic,0)
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
				res = self.process(experiment_dic,i)
				pre_detail = experiment_dic[i]
				for row in pre_detail:
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

	def process(self,experiment_dic,pre):
		res = 100.0
		mean_odds = {}
		if self.params['num_season']+pre in experiment_dic:
			for i in range(pre + 1,self.params['num_season'] + pre + 1):
				if experiment_dic[i]:
					pre_detail = experiment_dic[i]
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
					if self.params['with_neu']:
						if succ > 0:
							mean_odds[i] = float(succ + fail + neu) / float(succ)
						else:
							mean_odds[i] = 100.0
					else:
						if succ > 0:
							mean_odds[i] = 	float(succ + fail) / float(succ)
						else:
							mean_odds[i] = 100.0
		total_mean = 0.0
		max_num = 0
		mean_score = 100.0
		for i in mean_odds:
			total_mean += mean_odds[i]
			if mean_odds[i] > self.params['max_mean']:
				max_num += 1
		if len(mean_odds) > 0:
			mean_score = total_mean/len(mean_odds)
		if max_num >= self.params['max_rt']*len(mean_odds) or mean_score > self.params['mean_peak']:
			res = 100.0
		else:
			res = mean_score
		return res
