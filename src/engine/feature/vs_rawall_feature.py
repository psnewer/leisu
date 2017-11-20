#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import os
import pandas as pd
import gflags
import json
import copy
from abstract_feature import *

class VS_RAWALL_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'VS_RAWALL_FEATURE'
		self.params = {}
		self.params['length'] = 4
		self.params['levels'] = 3
		self.params['testrt'] = 0.1
	
	def execute_predict(self,league_id,serryid,df_serry,feature_log):
		team_res = []
		serryname = df_serry.iloc[-1]['serryname']
		train_dir = gflags.FLAGS.extract_path + str(league_id) + '/' + serryname
		if not os.path.exists(train_dir):
			return team_res
		sql_str = "select distinct serryid from Match where league_id=%d and serryname='%s' order by date desc"%(league_id,serryname)
		serries = pd.read_sql_query(sql_str,conn).head(2)
		serry_res = self.get_serryDic(league_id,serries)
		serry_feature = self.get_serryFeature(league_id,serries,serry_res)
		match_feature = self.get_predict_currentMatchFeature(league_id,df_serry,serry_res)
		res = self.predict_pack(league_id,df_serry,serries,serry_feature,match_feature,feature_log)
		for _res in res:
			res_str = json.dumps(_res,cls=GenEncoder)
			feature_log.write(res_str+'\n')
			team_res.append(_res)
		return team_res

	def execute_test(self,condition,feature_log):
		team_res = []
		league_id = int(condition[0].split('=')[1])
		serryname = condition[1].split('=')[1]
		train_dir = gflags.FLAGS.extract_path + str(league_id) + '/' + eval(serryname)
		if not os.path.exists(train_dir):
			return team_res
		cond_str = ' and '.join(condition)
		sql_str = "select distinct serryid from Match where %s order by date desc"%(cond_str)
		df_serry = pd.read_sql_query(sql_str,conn)
		serry_res = self.get_serryDic(league_id,df_serry)
		serry_feature = self.get_serryFeature(league_id,df_serry,serry_res)
		current_serryfeature = self.get_currentSerryFeature(league_id,df_serry,serry_res)
		match_feature = self.get_currentMatchFeature(league_id,df_serry,serry_res)
		res = self.test_pack(league_id,df_serry,serry_feature,current_serryfeature,match_feature,feature_log)
		for _res in res:
			res_str = json.dumps(_res,cls=GenEncoder)
			feature_log.write(res_str+'\n')
			team_res.append(_res)
		return team_res

	def get_serryFeature(self,league_id,df_serry,serry_res):
		res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = 'league_id=%d'%league_id + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			df = conciseDate(df)
			serry_dic = serry_res[serryid]
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				res[serryid][team] = {}
				team_dic = serry_res[serryid][team]
				level_res = {}
				level_res['self'] = {}
				level_res['to'] = {}
				for i in range(0,self.params['levels']):
					level_res['self'][i] = {}
					level_res['self'][i]['num'] = 0
					level_res['self'][i]['score'] = 0
					level_res['to'][i] = {}
					level_res['to'][i]['num'] = 0
					level_res['to'][i]['score'] = 0
				home_num = 0
				home_score = 0
				away_num = 0
				away_score = 0
				for date in team_dic:
					date_dic = team_dic[date]
					if date_dic['area'] == 1:
						home_num += 1
						home_score += date_dic['score']
					elif date_dic['area'] == 2:
						away_num += 1
						away_score += date_dic['score']
					level_dic = date_dic['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['self'][level]['num'] += 1
						level_res['self'][level]['score'] += date_dic['score']
					to_team = date_dic['toteam']
 					level_dic = serry_res[serryid][to_team][date]['to_feature']
					if -1 in level_dic:
 						level = level_dic[-1]['mom']
						level_res['to'][level]['num'] += 1
						level_res['to'][level]['score'] += date_dic['score']
				num = home_num + away_num
				num_score = home_score + away_score
				if num > 0:
					mean_score = float(num_score)/num
					if home_num > 0:
						home_mean = float(home_score)/home_num
					else:
						home_mean = mean_score
					if away_num > 0:
						away_mean = float(away_score)/away_num
					else:
						away_mean = mean_score
#					for level in level_res:
#						if level_res[level]['num'] > 0:
#							level_res[level]['score'] = float(level_res[level]['score'])/level_res[level]['num']
#						else:
#							level_res[level]['score'] = mean_score
					res[serryid][team]['level'] = level_res
					res[serryid][team]['home_score'] = home_score
					res[serryid][team]['home_num'] = home_num
					res[serryid][team]['away_score'] = away_score
					res[serryid][team]['away_num'] = away_num
		return res

	def get_currentSerryFeature(self,league_id,df_serry,serry_res):
		res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = 'league_id=%d'%league_id + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			df = conciseDate(df)
			serry_dic = serry_res[serryid]
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				res[serryid][team] = {}
				team_dic = serry_res[serryid][team]
				home_num = 0
				home_score = 0
				away_num = 0
				away_score = 0
				home_mean = 0.0
				away_mean = 0.0
				level_res = {}
				level_res['self'] = {}
				level_res['to'] = {}
				for i in range(0,self.params['levels']):
					level_res['self'][i] = {}
					level_res['to'][i] = {}
					level_res['self'][i]['num'] = 0
					level_res['to'][i]['num'] = 0
					level_res['self'][i]['score'] = 0
					level_res['to'][i]['score'] = 0
				_level_res = copy.deepcopy(level_res)
				dates = sorted(team_dic.keys())
				for date in dates:
					res[serryid][team][date] = {}
					res[serryid][team][date]['level'] = copy.deepcopy(_level_res)
					res[serryid][team][date]['home_score'] = home_score
					res[serryid][team][date]['home_num'] = home_num
					res[serryid][team][date]['away_score'] = away_score
					res[serryid][team][date]['away_num'] = away_num
					date_dic = team_dic[date]
					if date_dic['area'] == 1:
						home_num += 1
						home_score += date_dic['score']
					elif date_dic['area'] == 2:
						away_num += 1
						away_score += date_dic['score']
					level_dic = date_dic['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['self'][level]['num'] += 1
						level_res['self'][level]['score'] += date_dic['score']
					to_team = date_dic['toteam']
					level_dic = serry_res[serryid][to_team][date]['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['to'][level]['num'] += 1
						level_res['to'][level]['score'] += date_dic['score']
					_level_res = copy.deepcopy(level_res)
					num = home_num + away_num
					num_score = home_score + away_score
					if num > 0:
						mean_score = float(num_score)/num
						if home_num > 0:
							home_mean = float(home_score)/home_num
						else:
							home_mean = mean_score
						if away_num > 0:
							away_mean = float(away_score)/away_num
						else:
							away_mean = mean_score
#						for level in level_res:
#							if level_res[level]['num'] > 0:
#								_level_res[level]['score'] = float(level_res[level]['score'])/level_res[level]['num']
#							else:
#								_level_res[level]['score'] = mean_score
		return res

	def get_currentMatchFeature(self,league_id,df_serry,serry_res):
		res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = 'league_id=%d'%league_id + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			df = conciseDate(df)
			serry_dic = serry_res[serryid]
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				res[serryid][team] = {}
				df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team)).sort_values(by='date').reset_index()
				for idx,row in df_team.iterrows():
					if idx < self.params['length']:
						continue
					date = row['date']
					res[serryid][team][date] = {}
					level_res = {}
					for i in range(0,self.params['levels']):
						level_res[i] = {}
						level_res[i]['num'] = 0
						level_res[i]['score'] = 0
					home_num = 0
					home_score = 0
					away_num = 0
					away_score = 0
					for i in range(0,self.params['length']):
						torow = df_team.iloc[idx - i -1]
						todate = torow['date']
						toscore = serry_dic[team][todate]['score']
						if torow['home_team_id'] == team:
							toteam = torow['away_team_id']
							home_num += 1
							home_score += toscore
						else:
							toteam = torow['home_team_id']
							away_num += 1
							away_score += toscore
						if i not in serry_dic[toteam][todate]['to_feature'] or serry_dic[toteam][todate]['to_feature'][i]['todate'] >= date:
							continue
						tomom = serry_dic[toteam][todate]['to_feature'][i]['mom']
						level_res[tomom]['num'] += 1
						level_res[tomom]['score'] += toscore
					num = home_num + away_num
					num_score = home_score + away_score
					if num > 0:
						mean_score = float(num_score)/num
						if home_num > 0:
							home_mean = float(home_score)/home_num
						else:
							home_mean = mean_score
						if away_num > 0:
							away_mean = float(away_score)/away_num
						else:
							away_mean = mean_score
#						for level in level_res:
#							if level_res[level]['num'] > 0:
#								level_res[level]['score'] = float(level_res[level]['score'])/level_res[level]['num']
#							else:
#								level_res[level]['score'] = mean_score
						res[serryid][team][date]['level'] = level_res
						res[serryid][team][date]['home_score'] = home_score
						res[serryid][team][date]['home_num'] = home_num
						res[serryid][team][date]['away_score'] = away_score
						res[serryid][team][date]['away_num'] = away_num
		return res	
	
	def get_predict_currentMatchFeature(self,league_id,df_serry,serry_res):
		res = {}
		serryid = df_serry.iloc[-1]['serryid']
		serry_cond = "serryid='%s'"%serryid
		cond = 'league_id=%d'%league_id + ' and ' + serry_cond
		sql_str = "select * from Match where %s"%cond
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		serry_dic = serry_res[serryid]
		home_teams = df['home_team_id'].tolist()
		away_teams = df['away_team_id'].tolist()
		teams = set(home_teams + away_teams)
		for team in teams:
			res[team] = {}
			df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team)).sort_values(by='date',ascending=False).reset_index()
			level_res = {}
			for i in range(0,self.params['levels']):
				level_res[i] = {}
				level_res[i]['num'] = 0
				level_res[i]['score'] = 0
			home_num = 0
			home_score = 0
			away_num = 0
			away_score = 0
			if len(df_team) < self.params['length']:
				continue
			for i in range(0,self.params['length']):
				torow = df_team.iloc[i]
				todate = torow['date']
				toscore = serry_dic[team][todate]['score']
				if torow['home_team_id'] == team:
					toteam = torow['away_team_id']
					home_num += 1
					home_score += toscore
				else:
					toteam = torow['home_team_id']
					away_num += 1
					away_score += toscore
				if i not in serry_dic[toteam][todate]['to_feature']:
					continue
				tomom = serry_dic[toteam][todate]['to_feature'][i]['mom']
				level_res[tomom]['num'] += 1
				level_res[tomom]['score'] += toscore
			num = home_num + away_num
			num_score = home_score + away_score
			if num > 0:
				mean_score = float(num_score)/num
				if home_num > 0:
					home_mean = float(home_score)/home_num
				else:
					home_mean = mean_score
				if away_num > 0:
					away_mean = float(away_score)/away_num
				else:
					away_mean = mean_score
#				for level in level_res:
#					if level_res[level]['num'] > 0:
#						level_res[level]['score'] = float(level_res[level]['score'])/level_res[level]['num']
#					else:
#						level_res[level]['score'] = mean_score
				res[team]['level'] = level_res
				res[team]['home_score'] = home_score
				res[team]['home_num'] = home_num
				res[team]['away_score'] = away_score
				res[team]['away_num'] = away_num
		return res		

	def get_serryDic(self,league_id,df_serry):
		serry_res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			serry_res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = 'league_id=%d'%league_id + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			df = conciseDate(df)
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				serry_res[serryid][team] = self.get_teamInfo(df,team)
		return serry_res

	def get_teamInfo(self,df,team_id):
		df_team = df.query("home_team_id==%d | away_team_id==%d"%(team_id,team_id)).reset_index()
		df_team = df_team.sort_values(by='date')
		f1 = lambda x: 1 if x['home_team_id']==team_id else 2
		f2 = lambda x: 3 if (x['area']==1 and x['home_goal']>x['away_goal']) or (x['area']==2 and x['away_goal']>x['home_goal']) else 1 if x['home_goal']==x['away_goal'] else 0
		df_team['area'] = df_team.apply(f1, axis=1)
		df_team['score'] = df_team.apply(f2, axis=1)
		res = {}
		for idx,row in df_team.iterrows():
			date = row['date']
			res[date] = {}		
			res[date]['area'] = row['area']
			res[date]['score'] = row['score']
			if row['area'] == 1:
				res[date]['toteam'] = row['away_team_id']
			else:
				res[date]['toteam'] = row['home_team_id']
			res[date]['to_feature'] = {}
			for i in range(-1,self.params['length']):
				first = idx - self.params['length'] + 1 + i
				if first < 0:
					continue
				end = idx + i + 1
				if end > len(df_team):
					continue
				mom_score = 0
				todate = df_team.iloc[-1]['date']
				for j in range(first,end):
					torow = df_team.iloc[j]
					mom_score += torow['score']
				for level in range(0,self.params['levels']):
					if mom_score <= float(3*self.params['length'])/self.params['levels']*(level+1):
						res[date]['to_feature'][i] = {}
						res[date]['to_feature'][i]['mom'] = level
						res[date]['to_feature'][i]['todate'] = todate
						break
		return res

	def predict_pack(self,league_id,df_serry,serries,serry_feature,match_feature,feature_log):
		team_res = []
		serryid = df_serry.iloc[-1]['serryid']
		pre_serryid = 'none'
		for idx,serry in serries.iterrows():
			_serryid = serry['serryid']
			if _serryid != serryid:
				continue
			pre_idx = idx + 1
			if pre_idx >= len(serries):
				continue
			pre_serryid = serries.iloc[pre_idx]['serryid']
		if pre_serryid != 'none' and self.is_adjacent(serryid,pre_serryid,league_id):
			current_serry_feature = serry_feature[serryid]
			pre_serry_feature = serry_feature[pre_serryid]
			for ind,row in df_serry.iterrows():
				home_team_id = row['home_team_id']
				away_team_id = row['away_team_id']
				date = row['date']
				if home_team_id not in match_feature or away_team_id not in match_feature:
					continue
				if 'level' not in match_feature[home_team_id] or 'level' not in match_feature[away_team_id]:
					continue
				if home_team_id not in pre_serry_feature or away_team_id not in pre_serry_feature:
					continue
				home_match_score = []
				home_match_num = []
				for i in range(0,self.params['levels']):
					home_match_score.append(match_feature[home_team_id]['level'][i]['score'])
					home_match_num.append(match_feature[home_team_id]['level'][i]['num'])
				home_match_score.append(match_feature[home_team_id]['home_score'])
				home_match_num.append(match_feature[home_team_id]['home_num'])
				home_match_score.append(match_feature[home_team_id]['away_score'])
				home_match_num.append(match_feature[home_team_id]['away_num'])
				away_match_score = []
				away_match_num = []
				for i in range(0,self.params['levels']):
					away_match_score.append(match_feature[away_team_id]['level'][i]['score'])
					away_match_num.append(match_feature[away_team_id]['level'][i]['num'])
				away_match_score.append(match_feature[away_team_id]['home_score'])
				away_match_num.append(match_feature[away_team_id]['home_num'])
				away_match_score.append(match_feature[away_team_id]['away_score'])
				away_match_num.append(match_feature[away_team_id]['away_num'])
				home_current_self_serryscore = []
				home_current_self_serrynum = []
				home_current_to_serryscore = []
				home_current_to_serrynum = []
				for i in range(0,self.params['levels']):
					home_current_self_serryscore.append(current_serry_feature[home_team_id]['level']['self'][i]['score'])
					home_current_self_serrynum.append(current_serry_feature[home_team_id]['level']['self'][i]['num'])
					home_current_to_serryscore.append(current_serry_feature[home_team_id]['level']['to'][i]['score'])
					home_current_to_serrynum.append(current_serry_feature[home_team_id]['level']['to'][i]['num'])
				home_current_self_serryscore.append(current_serry_feature[home_team_id]['home_score'])
				home_current_self_serrynum.append(current_serry_feature[home_team_id]['home_num'])
				home_current_to_serryscore.append(current_serry_feature[home_team_id]['home_score'])
				home_current_to_serrynum.append(current_serry_feature[home_team_id]['home_num'])
				home_current_self_serryscore.append(current_serry_feature[home_team_id]['away_score'])
				home_current_self_serrynum.append(current_serry_feature[home_team_id]['away_num'])
				home_current_to_serryscore.append(current_serry_feature[home_team_id]['away_score'])
				home_current_to_serrynum.append(current_serry_feature[home_team_id]['away_num'])
				away_current_self_serryscore = []
				away_current_self_serrynum = []
				away_current_to_serryscore = []
				away_current_to_serrynum = []
				for i in range(0,self.params['levels']):
					away_current_self_serryscore.append(current_serry_feature[away_team_id]['level']['self'][i]['score'])
					away_current_self_serrynum.append(current_serry_feature[away_team_id]['level']['self'][i]['num'])
					away_current_to_serryscore.append(current_serry_feature[away_team_id]['level']['to'][i]['score'])
					away_current_to_serrynum.append(current_serry_feature[away_team_id]['level']['to'][i]['num'])
				away_current_self_serryscore.append(current_serry_feature[away_team_id]['home_score'])
				away_current_self_serrynum.append(current_serry_feature[away_team_id]['home_num'])
				away_current_to_serryscore.append(current_serry_feature[away_team_id]['home_score'])
				away_current_to_serrynum.append(current_serry_feature[away_team_id]['home_num'])
				away_current_self_serryscore.append(current_serry_feature[away_team_id]['away_score'])
				away_current_self_serrynum.append(current_serry_feature[away_team_id]['away_num'])
				away_current_to_serryscore.append(current_serry_feature[away_team_id]['away_score'])
				away_current_to_serrynum.append(current_serry_feature[away_team_id]['away_num'])
				home_pre_self_score = []
				home_pre_self_num = []
				home_pre_to_score = []
				home_pre_to_num = []
				for i in range(0,self.params['levels']):
					home_pre_self_score.append(pre_serry_feature[home_team_id]['level']['self'][i]['score'])
					home_pre_self_num.append(pre_serry_feature[home_team_id]['level']['self'][i]['num'])
					home_pre_to_score.append(pre_serry_feature[home_team_id]['level']['to'][i]['score'])
					home_pre_to_num.append(pre_serry_feature[home_team_id]['level']['to'][i]['num'])
				home_pre_self_score.append(pre_serry_feature[home_team_id]['home_score'])
				home_pre_self_num.append(pre_serry_feature[home_team_id]['home_num'])
				home_pre_to_score.append(pre_serry_feature[home_team_id]['home_score'])
				home_pre_to_num.append(pre_serry_feature[home_team_id]['home_num'])
				home_pre_self_score.append(pre_serry_feature[home_team_id]['away_score'])
				home_pre_self_num.append(pre_serry_feature[home_team_id]['away_num'])
				home_pre_to_score.append(pre_serry_feature[home_team_id]['away_score'])
				home_pre_to_num.append(pre_serry_feature[home_team_id]['away_num'])
				away_pre_self_score = []
				away_pre_self_num = []
				away_pre_to_score = []
				away_pre_to_num = []
				for i in range(0,self.params['levels']):
					away_pre_self_score.append(pre_serry_feature[away_team_id]['level']['self'][i]['score'])
					away_pre_self_num.append(pre_serry_feature[away_team_id]['level']['self'][i]['num'])
					away_pre_to_score.append(pre_serry_feature[away_team_id]['level']['to'][i]['score'])
					away_pre_to_num.append(pre_serry_feature[away_team_id]['level']['to'][i]['num'])
				away_pre_self_score.append(pre_serry_feature[away_team_id]['home_score'])
				away_pre_self_num.append(pre_serry_feature[away_team_id]['home_num'])
				away_pre_to_score.append(pre_serry_feature[away_team_id]['home_score'])
				away_pre_to_num.append(pre_serry_feature[away_team_id]['home_num'])
				away_pre_self_score.append(pre_serry_feature[away_team_id]['away_score'])
				away_pre_self_num.append(pre_serry_feature[away_team_id]['away_num'])
				away_pre_to_score.append(pre_serry_feature[away_team_id]['away_score'])
				away_pre_to_num.append(pre_serry_feature[away_team_id]['away_num'])
				home_res = [[home_match_score,home_current_self_serryscore,home_current_to_serryscore,home_pre_self_score,home_pre_to_score],[home_match_num,home_current_self_serrynum,home_current_to_serrynum,home_pre_self_num,home_pre_to_num]]
				away_res = [[away_match_score,away_current_self_serryscore,away_current_to_serryscore,away_pre_self_score,away_pre_to_score],[away_match_num,away_current_self_serrynum,away_current_to_serrynum,away_pre_self_num,away_pre_to_num]]
				res_home = {}
				res_home['date'] = date
				res_home['team_id'] = home_team_id
				res_home['area'] = 1
				res_home['toteam'] = away_team_id
				res_home[self.name] = home_res
				res_away = {}
				res_away['date'] = date
				res_away['team_id'] =away_team_id
				res_away['area'] = 2
				res_away['toteam'] = home_team_id
				res_away[self.name] = away_res
				team_res.append(res_home)
				team_res.append(res_away)
		return team_res

	def test_pack(self,league_id,df_serry,serry_feature,current_serryfeature,match_feature,feature_log):
		team_res = []
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			pre_idx = idx + 1
			if pre_idx >= len(df_serry):
				continue
			pre_serryid = df_serry.iloc[pre_idx]['serryid']
			if not self.is_adjacent(serryid,pre_serryid,league_id):
				continue
			serry_cond = "serryid='%s'"%serryid
			cond = "league_id=%d"%league_id + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn).sort_values(by='date')
			df = conciseDate(df)
			df['res'] = df.apply(lambda x: [0] if x['home_goal'] > x['away_goal'] else [1] if x['home_goal'] == x['away_goal'] else [2], axis=1)
			serry_match_feature = match_feature[serryid]
			serry_serry_feature = serry_feature[serryid]
			serry_current_serryfeature = current_serryfeature[serryid]
			pre_serry_feature = serry_feature[pre_serryid]
			for ind,row in df.iterrows():
				home_team_id = row['home_team_id']
				away_team_id = row['away_team_id']
				date = row['date']
				if home_team_id not in serry_match_feature or away_team_id not in serry_match_feature:
					continue
				if date not in serry_match_feature[home_team_id] or date not in serry_match_feature[away_team_id]:
					continue
				if 'level' not in serry_match_feature[home_team_id][date] or 'level' not in serry_match_feature[away_team_id][date]:
					continue
				if home_team_id not in pre_serry_feature or away_team_id not in pre_serry_feature:
					continue
				home_match_score = []
				home_match_num = []
				for i in range(0,self.params['levels']):
					home_match_score.append(serry_match_feature[home_team_id][date]['level'][i]['score'])
					home_match_num.append(serry_match_feature[home_team_id][date]['level'][i]['num'])
				home_match_score.append(serry_match_feature[home_team_id][date]['home_score'])
				home_match_num.append(serry_match_feature[home_team_id][date]['home_num'])
				home_match_score.append(serry_match_feature[home_team_id][date]['away_score'])
				home_match_num.append(serry_match_feature[home_team_id][date]['away_num'])
				away_match_score = []
				away_match_num = []
				for i in range(0,self.params['levels']):
					away_match_score.append(serry_match_feature[away_team_id][date]['level'][i]['score'])
					away_match_num.append(serry_match_feature[away_team_id][date]['level'][i]['num'])
				away_match_score.append(serry_match_feature[away_team_id][date]['home_score'])
				away_match_num.append(serry_match_feature[away_team_id][date]['home_num'])
				away_match_score.append(serry_match_feature[away_team_id][date]['away_score'])
				away_match_num.append(serry_match_feature[away_team_id][date]['away_num'])
				home_current_self_serryscore = []
				home_current_self_serrynum = []
				home_current_to_serryscore = []
				home_current_to_serrynum = []
				for i in range(0,self.params['levels']):
					home_current_self_serryscore.append(serry_current_serryfeature[home_team_id][date]['level']['self'][i]['score'])
					home_current_self_serrynum.append(serry_current_serryfeature[home_team_id][date]['level']['self'][i]['num'])
					home_current_to_serryscore.append(serry_current_serryfeature[home_team_id][date]['level']['to'][i]['score'])
					home_current_to_serrynum.append(serry_current_serryfeature[home_team_id][date]['level']['to'][i]['num'])
				home_current_self_serryscore.append(serry_current_serryfeature[home_team_id][date]['home_score'])
				home_current_self_serrynum.append(serry_current_serryfeature[home_team_id][date]['home_num'])
				home_current_to_serryscore.append(serry_current_serryfeature[home_team_id][date]['home_score'])
				home_current_to_serrynum.append(serry_current_serryfeature[home_team_id][date]['home_num'])
				home_current_self_serryscore.append(serry_current_serryfeature[home_team_id][date]['away_score'])
				home_current_self_serrynum.append(serry_current_serryfeature[home_team_id][date]['away_num'])
				home_current_to_serryscore.append(serry_current_serryfeature[home_team_id][date]['away_score'])
				home_current_to_serrynum.append(serry_current_serryfeature[home_team_id][date]['away_num'])
				away_current_self_serryscore = []
				away_current_self_serrynum = []
				away_current_to_serryscore = []
				away_current_to_serrynum = []
				for i in range(0,self.params['levels']):
					away_current_self_serryscore.append(serry_current_serryfeature[away_team_id][date]['level']['self'][i]['score'])
					away_current_self_serrynum.append(serry_current_serryfeature[away_team_id][date]['level']['self'][i]['num'])
					away_current_to_serryscore.append(serry_current_serryfeature[away_team_id][date]['level']['to'][i]['score'])
					away_current_to_serrynum.append(serry_current_serryfeature[away_team_id][date]['level']['to'][i]['num'])
				away_current_self_serryscore.append(serry_current_serryfeature[away_team_id][date]['home_score'])
				away_current_self_serrynum.append(serry_current_serryfeature[away_team_id][date]['home_num'])
				away_current_to_serryscore.append(serry_current_serryfeature[away_team_id][date]['home_score'])
				away_current_to_serrynum.append(serry_current_serryfeature[away_team_id][date]['home_num'])
				away_current_self_serryscore.append(serry_current_serryfeature[away_team_id][date]['away_score'])
				away_current_self_serrynum.append(serry_current_serryfeature[away_team_id][date]['away_num'])
				away_current_to_serryscore.append(serry_current_serryfeature[away_team_id][date]['away_score'])
				away_current_to_serrynum.append(serry_current_serryfeature[away_team_id][date]['away_num'])
				home_pre_self_score = []
				home_pre_self_num = []
				home_pre_to_score = []
				home_pre_to_num = []
				for i in range(0,self.params['levels']):
					home_pre_self_score.append(pre_serry_feature[home_team_id]['level']['self'][i]['score'])
					home_pre_self_num.append(pre_serry_feature[home_team_id]['level']['self'][i]['num'])
					home_pre_to_score.append(pre_serry_feature[home_team_id]['level']['to'][i]['score'])
					home_pre_to_num.append(pre_serry_feature[home_team_id]['level']['to'][i]['num'])
				home_pre_self_score.append(pre_serry_feature[home_team_id]['home_score'])
				home_pre_self_num.append(pre_serry_feature[home_team_id]['home_num'])
				home_pre_to_score.append(pre_serry_feature[home_team_id]['home_score'])
				home_pre_to_num.append(pre_serry_feature[home_team_id]['home_num'])
				home_pre_self_score.append(pre_serry_feature[home_team_id]['away_score'])
				home_pre_self_num.append(pre_serry_feature[home_team_id]['away_num'])
				home_pre_to_score.append(pre_serry_feature[home_team_id]['away_score'])
				home_pre_to_num.append(pre_serry_feature[home_team_id]['away_num'])
				away_pre_self_score = []
				away_pre_self_num = []
				away_pre_to_score = []
				away_pre_to_num = []
				for i in range(0,self.params['levels']):
					away_pre_self_score.append(pre_serry_feature[away_team_id]['level']['self'][i]['score'])
					away_pre_self_num.append(pre_serry_feature[away_team_id]['level']['self'][i]['num'])
					away_pre_to_score.append(pre_serry_feature[away_team_id]['level']['to'][i]['score'])
					away_pre_to_num.append(pre_serry_feature[away_team_id]['level']['to'][i]['num'])
				away_pre_self_score.append(pre_serry_feature[away_team_id]['home_score'])
				away_pre_self_num.append(pre_serry_feature[away_team_id]['home_num'])
				away_pre_to_score.append(pre_serry_feature[away_team_id]['home_score'])
				away_pre_to_num.append(pre_serry_feature[away_team_id]['home_num'])
				away_pre_self_score.append(pre_serry_feature[away_team_id]['away_score'])
				away_pre_self_num.append(pre_serry_feature[away_team_id]['away_num'])
				away_pre_to_score.append(pre_serry_feature[away_team_id]['away_score'])
				away_pre_to_num.append(pre_serry_feature[away_team_id]['away_num'])
				home_res = [[home_match_score,home_current_self_serryscore,home_current_to_serryscore,home_pre_self_score,home_pre_to_score],[home_match_num,home_current_self_serrynum,home_current_to_serrynum,home_pre_self_num,home_pre_to_num]]
				away_res = [[away_match_score,away_current_self_serryscore,away_current_to_serryscore,away_pre_self_score,away_pre_to_score],[away_match_num,away_current_self_serrynum,away_current_to_serrynum,away_pre_self_num,away_pre_to_num]]
				res_home = {}
				res_home['date'] = date
				res_home['team_id'] = home_team_id
				res_home['area'] = 1
				res_home['toteam'] = away_team_id
				res_home[self.name] = home_res
				res_away = {}
				res_away['date'] = date
				res_away['team_id'] =away_team_id
				res_away['area'] = 2
				res_away['toteam'] = home_team_id
				res_away[self.name] = away_res
				team_res.append(res_home)
				team_res.append(res_away)
		return team_res

	def is_adjacent(self,serryid,pre_serryid,league_id):
		league_cond = "league_id=%d"%league_id
		serry_cond = "serryid='%s'"%serryid
		cond = league_cond + ' and ' + serry_cond
		sql_str = "select * from Match where %s"%cond
		date = pd.read_sql_query(sql_str,conn).sort_values(by='date').iloc[-1]['date'][0:4]
		serry_cond = "serryid='%s'"%pre_serryid
		cond = league_cond + ' and ' + serry_cond
		sql_str = "select * from Match where %s"%cond
		pre_date = pd.read_sql_query(sql_str,conn).sort_values(by='date').iloc[-1]['date'][0:4]
		delta = int(date) - int(pre_date)
		if delta >= 2:
			return False
		else:
			return True
