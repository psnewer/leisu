#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class PRE_RANK_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'PRE_RANK_FEATURE'
		self.params = {}
		self.params['to_last'] = 3
	
	def execute_predict(self,league_id,serryid,df_serry,feature_log):
		team_res = []
		serryname = df_serry.iloc[-1]['serryname']
		sql_str = "select * from Match where league_id=%d and serryname='%s'"%(league_id,serryname)
		df_league = pd.read_sql_query(sql_str,conn)
		df_league = conciseDate(df_league)
		serries = df_league['serryid'].unique()
		res = self.process(df_serry,df_league,serries)
		for _res in res:
			res_str = json.dumps(_res,cls=GenEncoder)
			feature_log.write(res_str+'\n')
			team_res.append(_res)
		return team_res

	def execute_test(self,condition,feature_log):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		if len(df) < 1:
			return []
		df = df.sort_values(by='date')
		league_id = df.iloc[-1]['league_id']
		last_date = df.iloc[-1]['date']
		serryname = df.iloc[-1]['serryname']
		sql_str = "select * from Match where league_id=%d and serryname='%s' and date <= '%s'"%(league_id,serryname,last_date)
		df_league = pd.read_sql_query(sql_str,conn)
		df_league = conciseDate(df_league)
		df = conciseDate(df)
		serries = df_league['serryid'].unique()
		dates = df['date'].unique()
		team_res = []
		for date in dates:
			df_date = df.query("date=='%s'"%date)
			res = self.process(df_date,df_league,serries)
			for _res in res:
				res_str = json.dumps(_res,cls=GenEncoder)
				feature_log.write(res_str+'\n')
				team_res.append(_res)
		return team_res

	def process(self,df_date,df_league,serries):
		team_res = []
		for index,row in df_date.iterrows():
			home_team = row['home_team_id']
			away_team = row['away_team_id']	
			date = row['date']
			min_length = min(self.params['to_last'],len(serries))
			res_dic_home = {}
			res_dic_home['team_id'] = home_team
			res_dic_home['date'] = date
			res_dic_home['area'] = 1
			res_dic_home['toteam'] = away_team
			res_dic_home[self.name] = {}
			res_dic_away = {}
			res_dic_away['team_id'] = away_team
			res_dic_away['date'] = date
			res_dic_away['area'] = 2
			res_dic_away['toteam'] =home_team
			res_dic_away[self.name] = {}
			for i in range(0,min_length):
				serryid = serries[len(serries)-i-1]
				df_serry = df_league.query("serryid=='%s'"%serryid)
				df_home_team = df_serry.query("(home_team_id==%d | away_team_id == %d) & date < '%s'"%(home_team,home_team,date))
				df_away_team = df_serry.query("(home_team_id==%d | away_team_id == %d) & date < '%s'"%(away_team,away_team,date))	
				res_home = self.get_numandscore(df_home_team,home_team)
				res_away = self.get_numandscore(df_away_team,away_team)
				res_dic_home[self.name][i] = res_home
				res_dic_away[self.name][i] = res_away
			team_res.append(res_dic_home)
			team_res.append(res_dic_away)
		return team_res

	def get_numandscore(self,df_team,team):
		res = {}
		res['score'] = 0
		res['number'] = len(df_team)
		for index,row in df_team.iterrows():
			if (row['home_team_id'] == team):
				if (row['home_goal'] > row['away_goal']):
					res['score'] += 3
				elif(row['home_goal'] < row['away_goal']):
					res['score'] += 0
				else:
					res['score'] += 1
			else:
				if (row['home_goal'] > row['away_goal']):
					res['score'] += 0
				elif(row['home_goal'] < row['away_goal']):
					res['score'] += 3
				else:
					res['score'] += 1
		return res
