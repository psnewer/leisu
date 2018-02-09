# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import numpy as np
import gflags
import codecs
from feature_creator import *
from filter_creator import *
from tester_creator import *
from datetime import datetime,timedelta
from time import strptime
from min_goals_tester import *
from conf import *

class Fix_Predictor():
	def __init__(self,tester_creator):
		self.tester_creator = tester_creator
		self.experiments = {}
		f_exp = codecs.open(gflags.FLAGS.experiment_fix, 'r', encoding='utf-8')
		data_algs = json.load(f_exp)
		features = data_algs['features']
		filters = data_algs['filters']
		testers = data_algs['testers']
		experiments = data_algs['experiments']
		self.feature_creator = Feature_Creator(features)
		self.filter_creator = Filter_Creator(filters)
		self.tester_creator = Tester_Creator(testers,self.feature_creator,self.filter_creator)
		for exp in experiments:
			experiment_id = exp['experiment_id']
			experiment_name = exp['experiment_name']
			algs = exp['algs']
			testers = exp['testers']
			filters = exp['filters']
			self.experiments[experiment_id] = {}
			self.experiments[experiment_id]['experiment_name'] = experiment_name
			self.experiments[experiment_id]['feature'] = []
			self.experiments[experiment_id]['tester'] = []
			self.experiments[experiment_id]['filter'] = []
			for alg in algs:
				self.experiments[experiment_id]['feature'].append(alg)
			for filter in filters:
				self.experiments[experiment_id]['filter'].append(filter)
			for tester in testers:
				self.experiments[experiment_id]['tester'].append(tester)

	def predict(self):
		sql_str = "select * from TMatch"
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		leagues = df['league_id'].unique()
		fix_league_str = codecs.open(gflags.FLAGS.fix_leagues,'r',encoding='utf-8')
		fix_leagues = json.load(fix_league_str)
		fix_league_str.close()
		for league in leagues:
			cur.execute("SELECT name FROM League WHERE id = %d"%(league))
			league_name = cur.fetchone()[0]
			if league_name not in fix_leagues:
				continue
			predict_directory = gflags.FLAGS.predict_path + str(league)
			os.system(r'mkdir %s'%predict_directory)
			feature_res = predict_directory + '/feature_res.txt'
			predict_res = predict_directory + '/predict_res.txt'
			feature_log = codecs.open(feature_res,'w+',encoding='utf-8')
			predict_log = codecs.open(predict_res,'a+',encoding='utf-8')
			df_league = df.query("league_id==%d"%league)
			serries = df_league['serryid'].unique()
			for serry in serries:
				df_serry = df_league.query("serryid=='%s'"%serry)
				self.process(league,serry,df_serry,feature_log,predict_log)
			feature_log.close()
			predict_log.close()

	def pack(self):
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
					limi = predict_dic['limi']
					limi_with_neu = predict_dic['limi_with_neu']
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
						res['limi_with_neu'] = limi_with_neu
						res['home_team'] = home_team
						res['away_team'] = away_team
						team_res.append(res)
					if team_res:
						df_ele = pd.DataFrame(team_res).sort_values(by='date')
						df = df.append(df_ele)
		if len(df) > 0:
			df = df.groupby(['league']).apply(lambda x: x.sort_values(by='date')).reset_index(0,drop=True)
		df.to_csv(gflags.FLAGS.predict_summary,encoding="utf_8_sig")

	def process(self,league_id,serryid,df,feature_log,predict_log):
		features = []
		filters = []
		testers = []
		for exp_id in self.experiments:
			features.extend(self.experiments[exp_id]['feature'])
			filters.extend(self.experiments[exp_id]['filter'])
		features = list(set(features))
		filters = list(set(filters))
		self.tester_creator.setProcessor(features,filters)
		self.tester_creator.predict(league_id,serryid,df,feature_log)
		for exp_id in self.experiments:
			exp = self.experiments[exp_id]
			limi = 0.0
			limi_with_neu = limi
			filters = exp['filter']
			tester = exp['tester'][0]
			tester_kind = exp['experiment_name']
			df_filter = self.tester_creator.get_filtered(filters)
			self.analysis(df,df_filter,self.tester_creator.testers[tester],tester_kind,limi,limi_with_neu,exp_id,predict_log)
	
	def analysis(self,df,df_team,tester,tester_kind,limi,limi_with_neu,exp_id,predict_log):
		if len(df) < 1 or df_team is None or (df_team is not None and len(df_team) < 1):
			return
		league_id = df.iloc[-1]['league_id']
		serryname = df.iloc[-1]['serryname']
		team_res = {}
		team_res['league_id'] = league_id
		team_res['serryname'] = serryname
		team_res['tester'] = tester_kind
		team_res['limi'] = limi
		team_res['limi_with_neu'] = limi_with_neu
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
			if 1 in row:
				home_teams_posi = row[1]
			if 2 in row:
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
	
