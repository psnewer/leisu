#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
import copy
from abstract_trend import *

class ROUND_RT_TREND(ABSTRACT_TREND):
	def __init__(self):
		self.name = 'ROUND_RT_TREND'
		self.params = {}
		self.params['with_neu'] = False

	def execute_predict(self,dic_res,trend_log):
		date_res = []
		for experiment_id in dic_res:
			experiment_dic = dic_res[experiment_id]
			if 0 in experiment_dic and experiment_dic[0]:
				res = {}
				pre_detail = experiment_dic[0]
				df_pre = pd.DataFrame(pre_detail)	
				res[self.name] = self.get_rt(df_pre)
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
					dates = df_pre['date'].unique()
					length = len(dates)
					for i in range(0,length):
						date = dates[i]
						res = {}
						res['experiment_id'] = experiment_id
						res['pre'] = pre
						res['date'] = date
						df_date = df_pre[df_pre['date']<=date]
						res[self.name] = self.get_rt(df_date)
						date_res.append(res)
						res_str = json.dumps(res,cls=GenEncoder)
						trend_log.write(res_str+'\n')
		return date_res

	def get_rt(self,df_pre):
		res = {}
		row = df_pre.iloc[-1]
		home_team_id = row['home_team']
		away_team_id = row['away_team']
		date = row['date']
		league_id = row['league_id']
		serryname = row['serryname']
		serryid = row['serryid']
		cur.execute("SELECT season FROM Match WHERE league_id = %d and serryname = '%s' and serryid = %s"%(league_id,serryname,serryid))
		season = cur.fetchone()[0]
		cur.execute("SELECT distinct season FROM Match WHERE league_id = %d and serryname = '%s' and date <= '%s' order by date desc"%(league_id,serryname,date))
		seasons = cur.fetchall()
		if len(seasons) > 1:
			pre_season = seasons[1][0]
			cur.execute("SELECT count(*) FROM Match WHERE league_id = %d and serryname = '%s' and season = '%s' and date < '%s'"%(league_id,serryname,season,date))	
			now_num = cur.fetchone()[0]
			cur.execute("SELECT count(*), max(stage) FROM Match WHERE league_id = %d and serryname= '%s' and season = '%s'"%(league_id,serryname,pre_season))
			row = cur.fetchone()	
			num = row[0]
			rounds = row[1]
			res['rounds'] = rounds
			res['rt'] = float(now_num)/float(num)
			return res
