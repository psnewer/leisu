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
		self.params['with_neu'] = False
		self.params['num_season'] = 3
		self.params['peak_rt'] = 0.25

	def execute_predict(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			if 0 in experiment_dic and experiment_dic[0]:
				res = {}
				res['self.name'] = self.process(experiment_dic,0)
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
		peaks['first_peak'] = {}
		peaks['max_peak'] = {}
		if self.params['num_season'] + pre in experiment_dic:
			for i in range(pre + 1,self.params['num_season'] + pre + 1):
				if experiment_dic[i]:
					pre_detail = experiment_dic[i]
					df_team = pd.DataFrame(pre_detail)
					date_odds = self.get_every_date_odds(df_team)
					j = 0
					while j in date_odds and date_odds[j] == 0:
						 j += 1
					if j not in date_odds:
						continue
					pre_odds = date_odds[j]
					max_odds = 0.0
					num = 0
					for k in range(j+1,len(date_odds)):
						odds = date_odds[k]
						if odds > pre_odds:
							num += 1
						elif odds < pre_odds:
							if num >=2 or (num > 0 and pre_odds > 2.5):
								peaks['first_peak'][i] = pre_odds
							if num > 0:
								break
						pre_odds = odds
					for k in range(j+1,len(date_odds)):
						odds = date_odds[k]
						if odds > max_odds:
							max_odds = odds
					if max_odds > (1.0+self.params['peak_rt'])*date_odds[len(date_odds)-1]:
						peaks['max_peak'][i] = max_odds
		total_odds = 0.0
		for i in peaks['first_peak']:
			total_odds += peaks['first_peak'][i]
		if total_odds > 0.0:
			mean_first_peak = total_odds/len(peaks['first_peak'])
			min_delta = 100.0
			for i in peaks['first_peak']:
				delta = abs(mean_first_peak - peaks['first_peak'][i])
				if delta < min_delta or (delta==min_delta and peaks['first_peak'][i] > res['first_peak']):
					min_delta = delta
					res['first_peak'] = peaks['first_peak'][i]
		total_odds = 0.0
		for i in peaks['max_peak']:
			total_odds += peaks['max_peak'][i]
		if total_odds > 0.0:
			mean_max_peak = total_odds/len(peaks['max_peak'])
			min_delta = 100.0
			for i in peaks['max_peak']:
				delta = abs(mean_max_peak - peaks['max_peak'][i])
				if delta < min_delta or (delta==min_delta and peaks['max_peak'][i] > res['max_peak']):
					min_delta = delta
					res['max_peak'] = peaks['max_peak'][i]
		return res

	def get_every_date_odds(self,df_pre):
		dates = df_pre['date'].unique()
		length = len(dates)
		res = {}
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
			if self.params['with_neu']:
				if succ > 0:
					limi_odds_with_neu = float(succ + fail + neu) / float(succ)
					res[i] = limi_odds_with_neu
				else:
					res[i] = 0
			else:
				if succ > 0:
					limi_odds =  float(succ + fail) / float(succ)
					res[i] = limi_odds
				else:
					res[i] = 0
		return res
