#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class CURRENT_MATCH_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'CURRENT_MATCH_FEATURE'
		self.params = {}
		self.params['period']	= 5

	def execute_predict(self,league_id,serryid,df_serry,feature_log):
		sql_str = "select * from Match where league_id=%d and serryid='%s'"%(league_id,serryid)
		df = pd.read_sql_query(sql_str,conn)
		if len(df) < 1:
			return []
		df = conciseDate(df)
		team_res = []
		for idx,row in df_serry.iterrows():
			teams = [row['home_team_id'],row['away_team_id']]
			areas = [1,2]
			toteams = [row['away_team_id'],row['home_team_id']]
			date = row['date']
			for idx,team in enumerate(teams):
				df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
				df_team = df_team.sort_values(by='date')
				length = len(df_team)
				res_dic = {}
				res_dic['team_id'] = team
				res_dic['area'] = areas[idx]
				res_dic['toteam'] = toteams[idx]
				last = max(0,length - self.params['period'])
				df_stage = df_team[last:length]
				res_dic['date'] = date
				res = self.process(df_stage,team)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic,cls=GenEncoder)
				feature_log.write(res_str+'\n')
				team_res.append(res_dic)
		return team_res

	def execute_test(self,condition,feature_log):
		cond = list(condition)
		cond_str = ' and '.join(cond)
		sql_str = "select distinct serryid from Match where %s order by date desc"%(cond_str)
		df_serry = pd.read_sql_query(sql_str,conn)
		if gflags.FLAGS.update:
			df_serry = df_serry.head(1)
		team_res = []
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			cond[1] = "serryid='%s'"%serryid
			cond_str = ' and '.join(cond)
			sql_str = "select * from Match where %s"%(cond_str)
			df = pd.read_sql_query(sql_str,conn)
			if len(df) < 1:
				return []
			df = conciseDate(df)
			home_teams = df['home_team_id']
			away_teams = df['away_team_id']
			teams = home_teams.append(away_teams).unique()
			for team in teams:
				df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
				df_team = df_team.sort_values(by='date')
				length = len(df_team)
				for i in range(0,length):
					row = df_team.iloc[i]
					res_dic = {}
					res_dic['team_id'] = team
					if row['home_team_id'] == team:
						res_dic['area'] = 1
						res_dic['toteam'] = row['away_team_id']
					else:
						res_dic['area'] = 2
						res_dic['toteam'] = row['home_team_id']
					res_dic['date'] = row['date']
					df_stage = df_team[0:i]
					res = self.process(df_stage,team)
					res_dic[self.name] = res
					res_str = json.dumps(res_dic,cls=GenEncoder)
					feature_log.write(res_str+'\n')
					team_res.append(res_dic)
		return team_res

	def process(self,df,team):
		length = len(df)
		last = max(-1,length -1 - self.params['period'])
		res = {}
		for idx in range(length-1,last,-1):
			row = df.iloc[idx]
			pre = length - idx
			res[pre] = {}
			if (row['home_team_id'] == team):
				res[pre]['area'] = 1
				res[pre]['toteam'] = row['away_team_id']
				res[pre]['home_goal'] = row['home_goal']
				res[pre]['away_goal'] = row['away_goal']
			elif (row['away_team_id'] == team):
				res[pre]['area'] = 2
				res[pre]['toteam'] = row['home_team_id']
				res[pre]['home_goal'] = row['home_goal']
				res[pre]['away_goal'] = row['away_goal']
		return res
