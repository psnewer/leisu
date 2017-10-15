# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
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
		predict_summary = codecs.open(gflags.FLAGS.predict_summary,'w+',encoding='utf-8')
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
					for match_res in predict_dic['res']:
						res = {}
						home_team_id = match_res['home_team']
						away_team_id = match_res['away_team']
						cur.execute('SELECT name FROM Team WHERE id = ? ', (home_team_id, ))
						home_team = cur.fetchone()[0]
						cur.execute('SELECT name FROM Team WHERE id = ? ', (away_team_id, ))
						away_team = cur.fetchone()[0]
						res['league'] = league
						res['serryname'] = serryname
						res['tester'] = tester
						res['date'] = match_res['date']
						res['limi'] = limi
						res['home_team'] = home_team
						res['away_team'] = away_team
						team_res.append(res)
		json.dump(team_res, predict_summary, ensure_ascii=False)
		predict_summary.close()	

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
			filters = exp['filter']
			tester = exp['tester'][0]
			df_filter = self.tester_creator.get_filtered(filters)	
			self.analysis(df,df_filter,self.tester_creator.testers[tester],limi,predict_log)
	
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
		exp_cand = []
		for row in groupdata:
			exp_dic = {}
			group_dic = json.loads(row,encoding='utf-8')
			_serryname = group_dic['serryname']
			if (_serryname == serryname):
				c0_profit = group_dic['c0_profit']
				c0_p_total = group_dic['c0_p_succ'] + group_dic['c0_p_fail']
				c0_p_id = group_dic['c0_p_id']
				c1_profit = group_dic['c1_profit']
				c1_p_total = group_dic['c1_p_succ'] + group_dic['c1_p_fail']
				c1_p_id = group_dic['c1_p_id']
				c2_profit = group_dic['c2_profit']
				c2_p_total = group_dic['c2_p_succ'] + group_dic['c2_p_fail']
				c2_p_id = group_dic['c2_p_id']
				all_profit = group_dic['all_profit']
				all_p_id = group_dic['all_p_id']
				if (c0_profit > 0):
					if (c0_p_total > 0.1*match_now):
						exp_dic['exp_id'] = c0_p_id
						exp_dic['limi'] = float(c0_p_total)/float(group_dic['c0_p_succ'])
						exp_cand.append(exp_dic)
					elif ((c0_p_id==c1_p_id and c1_profit>0) or (c0_p_id==c2_p_id and c2_profit>0) or (c0_p_id==all_p_id and all_profit>0)):
						exp_dic['exp_id'] = c0_p_id
						exp_dic['limi'] = float(c0_p_total)/float(group_dic['c0_p_succ'])
						exp_cand.append(exp_dic)
					elif (c1_profit > 0):
						if (c1_p_total > 0.1 * match_pre1):
							exp_dic['exp_id'] = c1_p_id
							exp_dic['limi'] = float(c1_p_total)/float(group_dic['c1_p_succ'])
							exp_cand.append(exp_dic)
						elif ((c1_p_id==c2_p_id and c2_profit>0) or (c1_p_id==all_p_id and all_profit>0)):
							exp_dic['exp_id'] = c1_p_id
							exp_dic['limi'] = float(c1_p_total)/float(group_dic['c1_p_succ'])
							exp_cand.append(exp_dic)
						elif (c2_profit > 0):
							if (c2_p_total > 0.1 * match_pre2):
								exp_dic['exp_id'] = c2_p_id
								exp_dic['limi'] = float(c2_p_total)/float(group_dic['c2_p_succ'])
								exp_cand.append(exp_dic)
							elif (c2_p_id==all_p_id and all_profit>0):
								exp_dic['exp_id'] = c2_p_id
								exp_dic['limi'] = float(c2_p_total)/float(group_dic['c2_p_succ'])
								exp_cand.append(exp_dic)
				else:
					c0_id = group_dic['c0_id']
					c1_id = group_dic['c1_id']
					c2_id = group_dic['c2_id']
					all_id = group_dic['all_id']
					c0_total = group_dic['c0_succ'] + group_dic['c0_fail']
					c1_total = group_dic['c1_succ'] + group_dic['c1_fail']
					c2_total = group_dic['c2_succ'] + group_dic['c2_fail']
					if (c0_total > 0.1*match_now or (c0_id>0 and (c0_id==c1_id or c0_id==c2_id or c0_id==all_id))):
						exp_dic['exp_id'] = c0_id
						exp_dic['limi'] = float(c0_total)/float(group_dic['c0_succ'])
						exp_cand.append(exp_dic)
					elif (c1_total > 0.1*match_pre1 or (c1_id>0 and (c1_id==c2_id or c1_id==all_id))):
						exp_dic['exp_id'] = c1_id
						exp_dic['limi'] = float(c1_total)/float(group_dic['c1_succ'])
						exp_cand.append(exp_dic)
					elif (c2_total > 0.1*match_pre2 or (c2_id>0 and c2_id==all_id)):
						exp_dic['exp_id'] = c2_id
						exp_dic['limi'] = float(c2_total)/float(group_dic['c2_succ'])
						exp_cand.append(exp_dic)
		return exp_cand

	def analysis(self,df,df_team,tester,limi,predict_log):
		if len(df) < 1:
			return
		league_id = df.iloc[-1]['league_id']
		serryname = df.iloc[-1]['serryname']
		team_res = {}
		team_res['league_id'] = league_id
		team_res['serryname'] = serryname
		team_res['tester'] = tester.name
		team_res['limi'] = limi
		team_res['res'] = []
		res_list = []
		dates = df['date'].unique()
		for date in dates:
			df_date = df.query("date=='%s'"%date)
			date_dic = {}
			date_dic['date'] = date
			date_dic['home_teams'] = []
			date_dic['away_teams'] = []
			for index,row in df_date.iterrows():
				date_dic['home_teams'].append(row['home_team_id'])
				date_dic['away_teams'].append(row['away_team_id'])
			res_list.append(date_dic)
		df_date = pd.DataFrame(res_list)
		df_team = df_team[['date','team_id']]
		df_team = df_team.groupby("date",as_index=False).agg({'team_id': lambda x: list(x)})
		anadf = pd.merge(df_date,df_team,how='inner',on='date')
		succ = 0
		fail = 0
		for index,row in anadf.iterrows():
			date = row['date']
			team_id = row['team_id']
			home_teams = row['home_teams']
			away_teams = row['away_teams']
			if (tester.params['lateral'] == 1):
				for idx,home_team in enumerate(home_teams):
					away_team = away_teams[idx]
					if home_team in team_id or away_team in team_id:
						_res = {}
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						team_res['res'].append(_res)
			else:
				for idx,home_team in enumerate(home_teams):
					away_team = away_teams[idx]
					if home_team in team_id and away_team in team_id:
						_res = {}
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						team_res['res'].append(_res)
		dic_str = json.dumps(team_res,cls=GenEncoder,ensure_ascii=False)
		predict_log.write(dic_str+'\n')
	
