# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import numpy as np
import gflags
import codecs
import threading
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
			feature_log = codecs.open(feature_res,'w+',encoding='utf-8')
			predict_log = codecs.open(predict_res,'w+',encoding='utf-8')
			df_league = df.query("league_id==%d"%league)
			serries = df_league['serryid'].unique()
			for serry in serries:
				df_serry = df_league.query("serryid=='%s'"%serry)
				self.process(league,serry,df_serry,feature_log,predict_log)
			feature_log.close()
			predict_log.close()

	def pack(self):
		walks = os.walk(gflags.FLAGS.predict_path,topdown=False)
		team_res = []
		for root,dirs,files in walks:
			if 'predict_res.txt' in files:
				absfile = os.path.join(root,'predict_res.txt')
				predictdata = codecs.open(absfile,'r',encoding='utf-8')
				for row in predictdata:
					predict_dic = json.loads(row,encoding='utf-8')
					league_id = predict_dic['league_id']
					cur.execute('SELECT name FROM League WHERE id = ? ', (league_id, ))
					league = cur.fetchone()[0]
					serryname = predict_dic['serryname']
					tester = predict_dic['tester']
					limi = predict_dic['limi']
					limi_with_neu = predict_dic['limi_with_neu']
					profit = predict_dic['profit']
					mean_profit = predict_dic['mean_profit']
					index = predict_dic['index']
					for match_res in predict_dic['res']:
						res = {}
						home_team_id = match_res['home_team']
						away_team_id = match_res['away_team']
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
						res['profit'] = profit
						res['mean_profit'] = mean_profit
						res['index'] = index
						res['home_team'] = home_team
						res['away_team'] = away_team
						team_res.append(res)
		df = pd.DataFrame(team_res)
		if len(df) > 0:
			df = df.groupby('league').apply(lambda x: x.sort_values(by='date')).reset_index(0,drop=True)
		df.to_csv(gflags.FLAGS.predict_summary,encoding="utf_8_sig")

	def process(self,league_id,serryid,df,feature_log,predict_log):
		exp_ids = self.get_experiment(league_id,serryid)
		features = []
		filters = []
		testers = []
		for _exp in exp_ids:
			exp = self.experiments[_exp['exp_id']]
			features.extend(exp['feature'])
			filters.extend(exp['filter'])
		features = list(set(features))
		filters = list(set(filters))
		self.tester_creator.setProcessor(features,filters)
		self.tester_creator.predict(league_id,serryid,df,feature_log)
		for _exp in exp_ids:
			exp = self.experiments[_exp['exp_id']]
			limi = _exp['limi']
			limi_with_neu = _exp['limi_with_neu']
			profit = _exp['profit']
			mean_profit = _exp['mean_profit']
			index = _exp['index']
			filters = exp['filter']
			tester = exp['tester'][0]
			tester_kind = _exp['kind']
			df_filter = self.tester_creator.get_filtered(filters)
			self.analysis(df,df_filter,self.tester_creator.testers[tester],tester_kind,limi,limi_with_neu,profit,mean_profit,index,predict_log)
	
	def get_experiment(self,league_id,serryid):
		cur.execute("SELECT COUNT(*) FROM MATCH WHERE league_id = %d and serryid = '%s'"%(league_id,serryid))
		match_now = cur.fetchone()[0]
		if match_now == 0:
			return []
		cur.execute("SELECT serryname FROM TMATCH WHERE league_id = %d and serryid = '%s'"%(league_id, serryid))
		serryname = cur.fetchone()[0]
		cur.execute("SELECT distinct serryid  FROM MATCH WHERE league_id = %d AND serryname = '%s' AND serryid != '%s' ORDER BY date DESC"%(league_id, serryname, serryid))
		pre_serryids = cur.fetchall()
		pre_serryid1 = 0
		pre_serryid2 = 0
		match_pre1 = 0
		match_pre2 = 0
		if len(pre_serryids) > 0:
			pre_serryid1 = pre_serryids[0][0]
			cur.execute("SELECT COUNT(*) FROM MATCH WHERE league_id = %d and serryid = '%s'"%(league_id,pre_serryid1))	
			match_pre1 = cur.fetchone()[0]	
		if len(pre_serryids) > 1:
			pre_serryid2 = pre_serryids[1][0]
			cur.execute("SELECT COUNT(*) FROM MATCH WHERE league_id = %d and serryid = '%s'"%(league_id,pre_serryid2))
			match_pre2 = cur.fetchone()[0]
		group_path=gflags.FLAGS.group_path + str(league_id) + '/group_final.txt'
		if (not os.path.isfile(group_path)):
			return []
		groupdata = codecs.open(group_path,'r',encoding='utf-8')
		rt_data = codecs.open(gflags.FLAGS.rt_path,'r',encoding='utf-8')
		rt = json.load(rt_data)
		exp_cand = []
		for row in groupdata:
			group_dic = json.loads(row,encoding='utf-8')
			_serryname = group_dic['serryname']
			kind = group_dic['kind']
			if (_serryname == serryname):
				c0_profit = group_dic['c0_profit']
				c0_id_with_neu = group_dic['c0_id_with_neu']
				if len(c0_profit) > 0:
					sorted_ind = np.argsort(c0_profit).tolist()
					sorted_ind.reverse()
					for idx,ind in enumerate(sorted_ind):
						exp_dic = {}
						exp_dic['kind'] = group_dic['kind']
						exp_dic['exp_id'] = group_dic['c0_p_id'][ind]
						exp_dic['limi'] = group_dic['c0_p_limi'][ind]
						exp_dic['limi_with_neu'] = group_dic['c0_p_limi_with_neu'][ind]
						exp_dic['profit'] = group_dic['c0_profit'][ind]
						exp_dic['mean_profit'] = group_dic['c0_meanProfit'][ind]
						exp_dic['index'] = idx
						exp_cand.append(exp_dic)
				if c0_id_with_neu > 0:
					exp_dic = {}
					exp_dic['kind'] = group_dic['kind']
					exp_dic['exp_id'] = c0_id_with_neu
					exp_dic['limi'] = group_dic['c0_limi']
					exp_dic['limi_with_neu'] = group_dic['c0_limi_with_neu']
					exp_dic['profit'] = group_dic['c0_limi_profit']
					exp_dic['mean_profit'] = group_dic['c0_limi_meanProfit']
					exp_dic['index'] = -1
#					if str(league_id) in rt and serryname in rt[str(league_id)]:
#						if kind.startswith('min') and 'min_rt' in rt[str(league_id)][serryname] and rt[str(league_id)][serryname]['min_rt'] > 0.0:
#							exp_dic['limi'] = 1.0/rt[str(league_id)][serryname]['min_rt']
#						elif kind.startswith('may') and 'may_rt' in rt[str(league_id)][serryname] and rt[str(league_id)][serryname]['may_rt'] > 0.0:
#							exp_dic['limi'] = 1.0/rt[str(league_id)][serryname]['may_rt']
#					else:
#						exp_dic['limi'] = 0.0
					exp_cand.append(exp_dic)
		return exp_cand

	def analysis(self,df,df_team,tester,tester_kind,limi,limi_with_neu,profit,mean_profit,index,predict_log):
		if len(df) < 1 or len(df_team) < 1:
			return
		league_id = df.iloc[-1]['league_id']
		serryname = df.iloc[-1]['serryname']
		team_res = {}
		team_res['league_id'] = league_id
		team_res['serryname'] = serryname
		team_res['tester'] = tester_kind
		team_res['limi'] = limi
		team_res['limi_with_neu'] = limi_with_neu
		team_res['profit'] = profit
		team_res['mean_profit'] = mean_profit
		team_res['index'] = index
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
					if (home_teams_posi is not None and away_teams_posi is not None) and (home_team in home_teams_posi or away_team in away_teams_posi):
						_res = {}
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						team_res['res'].append(_res)
			else:
				for idx,home_team in enumerate(home_teams):
					away_team = away_teams[idx]
					if (home_teams_posi is not None and away_teams_posi is not None) and (home_team in home_teams_posi and away_team in away_teams_posi):
						_res = {}
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						team_res['res'].append(_res)
		dic_str = json.dumps(team_res,cls=GenEncoder,ensure_ascii=False)
		predict_log.write(dic_str+'\n')
	
