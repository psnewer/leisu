# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import numpy as np
import gflags
import codecs
from datetime import datetime,timedelta
from time import strptime
from min_goals_tester import *
from conf import *

class Predictor():
	def __init__(self,tester_creator,experiments):
		self.tester_creator = tester_creator
		self.experiments = experiments

	def predict(self):
		sql_str = "select * from TMatch"
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		leagues = df['league_id'].unique()
		for league in leagues:
			predict_directory = gflags.FLAGS.predict_path + str(league)
			os.system(r'mkdir %s'%predict_directory)
			feature_res = predict_directory + '/feature_res.txt'
			predict_res = predict_directory + '/predict_res.txt'
			predict_sres = predict_directory + '/predict_sres.txt'
			feature_log = codecs.open(feature_res,'w+',encoding='utf-8')
			predict_log = codecs.open(predict_res,'a+',encoding='utf-8')
			predict_slog = codecs.open(predict_sres,'a+',encoding='utf-8')
			df_league = df.query("league_id==%d"%league)
			serries = df_league['serryid'].unique()
			for serry in serries:
				df_serry = df_league.query("serryid=='%s'"%serry)
				self.process(league,serry,df_serry,feature_log,predict_log,predict_slog)
			feature_log.close()
			predict_log.close()
			predict_slog.close()

	def pack(self):
		self.pack_b()
		self.pack_s()

	def pack_b(self):
		walks = os.walk(gflags.FLAGS.predict_path,topdown=False)
		df = pd.DataFrame([])
		for root,dirs,files in walks:
			if 'predict_res.txt' in files:
				absfile = os.path.join(root,'predict_res.txt')
				predictdata = codecs.open(absfile,'r',encoding='utf-8')
				for row in predictdata:
					team_res = []
					predict_dic = json.loads(row,encoding='utf-8')
					league_id = predict_dic['league_id']
					cur.execute('SELECT name FROM League WHERE id = ? ', (league_id, ))
					league = cur.fetchone()[0]
					serryname = predict_dic['serryname']
					tester = predict_dic['tester']
#					limi = predict_dic['limi']
					test_id_dic = predict_dic['test_id']
					for match_res in predict_dic['res']:
						for test_id in test_id_dic:
							limi = test_id_dic[test_id]
							res = {}
							home_team_id = match_res['home_team_id']
							away_team_id = match_res['away_team_id']
							cur.execute('SELECT name FROM Team WHERE id = ? ', (home_team_id, ))
							home_team = cur.fetchone()[0]
							cur.execute('SELECT name FROM Team WHERE id = ? ', (away_team_id, ))
							away_team = cur.fetchone()[0]
							cur.execute('SELECT date FROM TMatch WHERE home_team_id = ? and away_team_id = ? ', (home_team_id, away_team_id, ))
							date = cur.fetchone()[0]
							res['league'] = league
							res['serryname'] = serryname
							res['tester'] = tester
							res['date'] = date
							res['limi'] = limi
							res['test_id'] = test_id
							res['home_team'] = home_team
							res['away_team'] = away_team
							team_res.append(res)
					if team_res:
						df_ele = pd.DataFrame(team_res).sort_values(by='date')
						df = df.append(df_ele)
		if len(df) > 0:
			df = df.groupby(['home_team','away_team','date','tester','test_id']).apply(lambda x: x.sort_values(by='limi').iloc[0]).reset_index(drop=True)
			df = df.sort_values(by=['date','league']).reset_index(0,drop=True)
		df.to_csv(gflags.FLAGS.predict_summary,encoding="utf_8_sig")

	def pack_s(self):
		walks = os.walk(gflags.FLAGS.predict_path,topdown=False)
		df = pd.DataFrame([])
		for root,dirs,files in walks:
			if 'predict_sres.txt' in files:
				absfile = os.path.join(root,'predict_sres.txt')
				predictdata = codecs.open(absfile,'r',encoding='utf-8')
				for row in predictdata:
					team_res = []
					predict_dic = json.loads(row,encoding='utf-8')
					league_id = predict_dic['league_id']
					cur.execute('SELECT name FROM League WHERE id = ? ', (league_id, ))
					league = cur.fetchone()[0]
					serryname = predict_dic['serryname']
					tester = predict_dic['tester']
					limi = predict_dic['limi']
					test_id = predict_dic['test_id']
					for match_res in predict_dic['res']:
						res = {}
						home_team_id = match_res['home_team_id']
						away_team_id = match_res['away_team_id']
						cur.execute('SELECT name FROM Team WHERE id = ? ', (home_team_id, ))
						home_team = cur.fetchone()[0]
						cur.execute('SELECT name FROM Team WHERE id = ? ', (away_team_id, ))
						away_team = cur.fetchone()[0]
						cur.execute('SELECT date FROM TMatch WHERE home_team_id = ? and away_team_id = ? ', (home_team_id, away_team_id, ))
						date = cur.fetchone()[0]
						res['league'] = league
						res['serryname'] = serryname
						res['tester'] = tester
						res['date'] = date
						res['limi'] = limi
						res['test_id'] = test_id
						res['home_team'] = home_team
						res['away_team'] = away_team
						team_res.append(res)
					if team_res:
						df_ele = pd.DataFrame(team_res).sort_values(by='date')
						date_1 = df_ele.iloc[0]['date']
						date_2 = (datetime.strptime(date_1, '%Y%m%d%H%M') + timedelta(hours=8)).strftime('%Y%m%d%H%M')
						df_ele = df_ele[df_ele['date'] <= date_2]
						df = df.append(df_ele)
		if len(df) > 0:
			df = df.groupby(['home_team','away_team','date','tester']).apply(lambda x: x.sort_values(by='limi',ascending = False).iloc[0]).reset_index(drop=True)
			df = df.groupby(['league']).apply(lambda x: x.sort_values(by='date')).reset_index(0,drop=True)
		df.to_csv(gflags.FLAGS.predict_s_summary,encoding="utf_8_sig")

	def process(self,league_id,serryid,df,feature_log,predict_log,predict_slog):
		exp_ids = self.get_experiment(league_id,serryid)
		self.execute(exp_ids,league_id,serryid,df,feature_log,predict_log)
		exp_ids = self.get_sexperiment(league_id,serryid)
		self.execute(exp_ids,league_id,serryid,df,feature_log,predict_slog)

	def execute(self,exp_ids,league_id,serryid,df,feature_log,predict_log):
		features = []
		filters = []
		testers = []
		for _exp in exp_ids:
			exp = self.experiments[int(_exp)]
			features.extend(exp['feature'])
			filters.extend(exp['filter'])
		features = list(set(features))
		filters = list(set(filters))
		self.tester_creator.setProcessor(features,filters)
		self.tester_creator.predict(league_id,serryid,df,feature_log)
		for exp_id_str in exp_ids:
			exp_id = int(exp_id_str)
			exp_dic = exp_ids[exp_id_str]
			exp = self.experiments[exp_id]
			test_id = exp_dic['test_id']
			filters = exp['filter']
			tester = exp['tester'][0]
			tester_kind = exp_dic['kind']
			df_filter = self.tester_creator.get_filtered(filters)
			self.analysis(df,df_filter,self.tester_creator.testers[tester],tester_kind,test_id,exp_id,predict_log)
	
	def get_experiment(self,league_id,serryid):
		cur.execute("SELECT serryname FROM TMATCH WHERE league_id = %d and serryid = '%s'"%(league_id, serryid))
		serryname = cur.fetchone()[0]
		trend_path=gflags.FLAGS.group_path + str(league_id) + '/' + serryname + '/trend_final.txt'
		if (not os.path.isfile(trend_path)):
			return []
		kind_path = gflags.FLAGS.kind_file
		kind_data = codecs.open(kind_path,'r',encoding='utf-8')
		kind_list = json.load(kind_data)
		trend_data = codecs.open(trend_path,'r',encoding='utf-8')	
		exp_cand = json.load(trend_data)
		for exp_id in exp_cand:
			for kind_name in kind_list:
				exps = kind_list[kind_name]
				if int(exp_id) in exps:
					exp_cand[exp_id]['kind'] = kind_name
					break
		kind_data.close()
		trend_data.close()	
		return exp_cand

	def get_sexperiment(self,league_id,serryid):
		cur.execute("SELECT serryname FROM TMATCH WHERE league_id = %d and serryid = '%s'"%(league_id, serryid))
		serryname = cur.fetchone()[0]
		trend_path=gflags.FLAGS.group_path + str(league_id) + '/' + serryname + '/strend_final.txt'
		if (not os.path.isfile(trend_path)):
			return []
		kind_path = gflags.FLAGS.kind_file
		kind_data = codecs.open(kind_path,'r',encoding='utf-8')
		kind_list = json.load(kind_data)
		trend_data = codecs.open(trend_path,'r',encoding='utf-8')	
		exp_cand = json.load(trend_data)
		for exp_id in exp_cand:
			for kind_name in kind_list:
				exps = kind_list[kind_name]
				if int(exp_id) in exps:
					exp_cand[exp_id]['kind'] = kind_name
					break
		kind_data.close()
		trend_data.close()	
		return exp_cand

	def analysis(self,df,df_team,tester,tester_kind,test_id,exp_id,predict_log):
		if len(df) < 1 or df_team is None or (df_team is not None and len(df_team) < 1):
			return
		league_id = df.iloc[-1]['league_id']
		serryname = df.iloc[-1]['serryname']
		team_res = {}
		team_res['league_id'] = league_id
		team_res['serryname'] = serryname
		team_res['tester'] = tester_kind
#		team_res['limi'] = limi
		team_res['test_id'] = test_id
		team_res['exp_id'] = exp_id
		team_res['res'] = []
		res_list = []
		dates = df['date'].unique()
		for date in dates:
			df_date = df.query("date=='%s'"%date)
			date_dic = {}
			date_dic['date'] = date
			date_dic['home_teams'] = []
			date_dic['away_teams'] = []
			for idx,row in df_date.iterrows():
				date_dic['home_teams'].append(row['home_team_id'])
				date_dic['away_teams'].append(row['away_team_id'])
			res_list.append(date_dic)
		df_date = pd.DataFrame(res_list)
		df_team = df_team[['date','team_id','area']]
		df_team = df_team.groupby(['date','area']).apply(lambda x: list(x['team_id'])).unstack('area').reset_index(0)
		anadf = pd.merge(df_date,df_team,how='inner',on='date')
		for idx,row in anadf.iterrows():
			date = row['date']
			home_teams_posi = None
			away_teams_posi = None
			if 1 in row and row[1]==row[1]:
				home_teams_posi = row[1]
			if 2 in row and row[2]==row[2]:
				away_teams_posi = row[2]
			home_teams = row['home_teams']
			away_teams = row['away_teams']
			if (tester.params['lateral'] == 1):
				for idx,home_team in enumerate(home_teams):
					away_team = away_teams[idx]
					if (home_teams_posi is not None and home_team in home_teams_posi) or (away_teams_posi is not None and away_team in away_teams_posi):
						_res = {}
						_res['date'] = date
						_res['home_team_id'] = home_team
						_res['away_team_id'] = away_team
						team_res['res'].append(_res)
			else:
				for idx,home_team in enumerate(home_teams):
					away_team = away_teams[idx]
					if (home_teams_posi is not None and away_teams_posi is not None) and (home_team in home_teams_posi and away_team in away_teams_posi):
						_res = {}
						_res['date'] = date
						_res['home_team_id'] = home_team
						_res['away_team_id'] = away_team
						team_res['res'].append(_res)
		dic_str = json.dumps(team_res,cls=GenEncoder,ensure_ascii=False)
		predict_log.write(dic_str+'\n')
	
