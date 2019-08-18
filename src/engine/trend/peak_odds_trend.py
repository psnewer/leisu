#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
import copy
from abstract_trend import *

class PEAK_ODDS_TREND(ABSTRACT_TREND):
	def __init__(self):
		self.name = 'PEAK_ODDS_TREND'
		self.params = {}
		self.params['bottom'] = 1.4
		self.params['num_season'] = 4

	def execute_predict(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			if 0 in experiment_dic and experiment_dic[0]:
				res = {}
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
					res_detail['experiment_id'] = experiment_id
					res_detail['pre'] = i
					res_detail['date'] = date
					res_detail[self.name] = res
					date_res.append(res_detail)
					res_str = json.dumps(res_detail,cls=GenEncoder)
					trend_log.write(res_str+'\n')
		return date_res

	def process(self,experiment_dic,pre):
		res = {}
		peaks = {}
		peaks['limi_odds'] = {}
		peaks['limi_odds_with_neu'] = {}
		peaks['limi_odds']['max_odds'] = {}
		peaks['limi_odds']['min_odds'] = {}
		peaks['limi_odds_with_neu']['max_odds'] = {}
		peaks['limi_odds_with_neu']['min_odds'] = {}
		if self.params['num_season'] + pre in experiment_dic:
			for i in range(pre + 1,self.params['num_season'] + pre + 1):
				if experiment_dic[i]:
					pre_detail = experiment_dic[i]
					df_team = pd.DataFrame(pre_detail)
					date_odds = self.get_every_date_odds(df_team)['limi_odds']
					j = 0
					while j in date_odds and date_odds[j] == 0:
						 j += 1
					if j not in date_odds:
						continue
					pre_odds = date_odds[j]
					max_odds = 0.0
					min_odds = 100.0
					for k in range(j+1,len(date_odds)):
						odds = date_odds[k]
						if odds < min_odds and max_odds > self.params['bottom']:
							min_odds = odds
						elif odds > max_odds:
							max_odds = odds
					if max_odds > 0.0:
						peaks['limi_odds']['max_odds'][i] = max_odds
					if min_odds < 100.0:
						peaks['limi_odds']['min_odds'][i] = min_odds
			for i in range(pre + 1,self.params['num_season'] + pre + 1):
				if experiment_dic[i]:
					pre_detail = experiment_dic[i]
					df_team = pd.DataFrame(pre_detail)
					date_odds = self.get_every_date_odds(df_team)['limi_odds_with_neu']
					j = 0
					while j in date_odds and date_odds[j] == 0:
						 j += 1
					if j not in date_odds:
						continue
					pre_odds = date_odds[j]
					max_odds = 0.0
					min_odds = 100.0
					for k in range(j+1,len(date_odds)):
						odds = date_odds[k]
						if odds < min_odds and max_odds > self.params['bottom']:
							min_odds = odds
						elif odds > max_odds:
							max_odds = odds
					if max_odds > 0.0:
						peaks['limi_odds_with_neu']['max_odds'][i] = max_odds
					if min_odds < 100.0:
						peaks['limi_odds_with_neu']['min_odds'][i] = min_odds
		return peaks

	def get_every_date_odds(self,df_pre):
		dates = df_pre['date'].unique()
		length = len(dates)
		res = {}
		res['limi_odds'] = {}
		res['limi_odds_with_neu'] = {}
		for i in range(0, length):
			date = dates[i]
			df_date = df_pre[df_pre['date']<=date]
			per_count = df_date['res'].value_counts()
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
				res['limi_odds'][i] = limi_odds
				res['limi_odds_with_neu'][i] = limi_odds_with_neu
			else:
				res['limi_odds'][i] = 0
				res['limi_odds_with_neu'][i] = 0
		return res
