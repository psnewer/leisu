#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
import copy
from abstract_trend import *

class ASC_TIME_TREND(ABSTRACT_TREND):
	def __init__(self):
		self.name = 'ASC_TIME_TREND'
		self.params = {}

	def execute_predict(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			if 0 in experiment_dic and experiment_dic[0]:
				res = {}
				pre_detail = experiment_dic[0]
				df_pre = pd.DataFrame(pre_detail)	
				pre_detail = self.get_every_date_odds(df_pre)
				res[self.name] = self.process(pre_detail,-1)
				res['experiment_id'] = experiment_id
				date_res.append(res)
				res_str = json.dumps(res,cls=GenEncoder)
				trend_log.write(res_str+'\n')
		return date_res

	def execute_test(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			for pre in experiment_dic:
				if experiment_dic[pre]:
					pre_detail = experiment_dic[pre]
					df_pre = pd.DataFrame(pre_detail)
					pre_detail = self.get_every_date_odds(df_pre)
					dates = df_pre['date'].unique()
					length = len(dates)
					for i in range(0,length):
						date = dates[i]
						res = {}
						res['experiment_id'] = experiment_id
						res['pre'] = pre
						res['date'] = date
						res[self.name] = self.process(pre_detail,i)
						date_res.append(res)
						res_str = json.dumps(res,cls=GenEncoder)
						trend_log.write(res_str+'\n')
		return date_res

	def process(self,res_detail,_ind):
		asc = {}
		length = len(res_detail['limi_odds'])
		ind = _ind
		if ind < 0:
			ind = len(res_detail['limi_odds'])
		res = {}
		for i in range(1,ind+1):
			res[i] = res_detail['limi_odds'][length-ind+i]
		if 1 in res:
			pre_odds = res[1]
		score = 0
		for i in range(2,len(res)+1):
			odds = res[i]
			if odds == 100.0:
				break
			if odds > pre_odds:
				if score <= 0:
					score -= 1
				else:
					break
			elif odds < pre_odds:
				if score >= 0:
					score += 1
				else:
					break
			pre_odds = odds
		asc['limi_odds'] = score
		length = len(res_detail['limi_odds_with_neu'])
		ind = _ind
		if ind < 0:
			ind = len(res_detail['limi_odds_with_neu'])
		res = {}
		for i in range(1,ind+1):
			res[i] = res_detail['limi_odds_with_neu'][length-ind+i]
		if 1 in res:
			pre_odds = res[1]
		score = 0
		for i in range(2,len(res)+1):
			odds = res[i]
			if odds == 100.0:
				break
			if odds > pre_odds:
				if score <= 0:
					score -= 1
				else:
					break
			elif odds < pre_odds:
				if score >= 0:
					score += 1
				else:
					break
			pre_odds = odds
		asc['limi_odds_with_neu'] = score
		return asc
		
	def get_every_date_odds(self,df_pre):
		dates = df_pre['date'].unique()
		length = len(dates)
		res = {}
		res['limi_odds'] = {}
		res['limi_odds_with_neu'] = {}
		for i in range(length - 1, -1, -1):
			date = dates[i]
			df_date_all = df_pre[df_pre['date']<=date]
			per_count_all = df_date_all['res'].value_counts()
			succ = 0
			fail = 0
			neu = 0
			if 0 in per_count_all:
				neu = per_count_all[0]
			if 1 in per_count_all:
				succ = per_count_all[1]
			if 2 in per_count_all:
				fail = per_count_all[2]
			if succ > 0:
				limi_odds =  float(succ + fail) / float(succ)
				limi_odds_with_neu = float(succ + fail + neu) / float(succ)
				res['limi_odds'][length - i] = limi_odds
				res['limi_odds_with_neu'][length - i] = limi_odds_with_neu
			else:
				res['limi_odds'][length - i] = 100.0
				res['limi_odds_with_neu'][length - i] = 100.0
		return res
			
