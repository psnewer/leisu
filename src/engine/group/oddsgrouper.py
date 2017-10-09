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

class OddsGrouper():
	def __init__(self,tester_creator,experiments,condition):
		self.tester_creator = tester_creator
		self.experiments = experiments
		condition = json.loads(condition)
		league_str = str(condition['league_id'])
		self.conditions = []
		league_cond = 'league_id=' + league_str
		for serryid in condition['serryid']:
			serry_cond = 'serryid=' + str(serryid)
			self.conditions.append([league_cond,serry_cond])
		group_directory = gflags.FLAGS.group_path + league_str
		os.system(r'mkdir %s'%group_directory)
		self.group_detail = group_directory + '/group_detail.txt'
		self.group_final = group_directory + '/group_final.txt'
		self.feature_res = group_directory + '/feature_res.txt'
		self.test_res = group_directory + '/test_res.txt'

	def group(self):
		self.process()
		self.analysis()

	def process(self):
		feature_res = open(self.feature_res,'w+')
		test_res = open(self.test_res,'w+')
		for cond in self.conditions:
			self.tester_creator.group(cond,feature_log=feature_res)
			for experiment_id in self.experiments:
				GlobalVar.set_experimentId(experiment_id)
				exp = self.experiments[experiment_id]
				features = exp['feature']
				filters = exp['filter']
				testers = exp['tester']
				self.tester_creator.test(filters,testers,cond,tester_log=test_res)
		feature_res.close()
		test_res.close()

	def analysis(self):
		f_kind = open(gflags.FLAGS.kind_file,'r')
		group_final = open(self.group_final,'w+')
		test_res = open(self.test_res,'r')
		kinds = json.load(f_kind)
		res_list = []
		profit = 0.0
		for row in test_res:
			res_dic = json.loads(row)
			if 'experiment_id' in res_dic:
				res_dic['profit'] = profit
				profit = 0.0
				res_list.append(res_dic)
			else:
				profit += self.getProfit(res_dic)
		df =pd.DataFrame(res_list)[['league_id','serryname','last_date','experiment_id','success','failure','profit']]
		df.to_csv(self.group_detail,encoding='utf-8')
		leagues = df['league_id'].unique()
		for league in leagues:
			df_league = df.query("league_id==%d"%league)
			serrynames = df_league['serryname'].unique()
			for serryname in serrynames:
				df_serryname = df_league[df_league.serryname==serryname]
				for kind in kinds:
					kind_name = kind['name']
					exps = kind['exps']
					min_all = 100.0
					min_c0 = 100.0
					min_c1 = 100.0
					min_c2 = 100.0
					max_allprofit = 0.0
					max_c0profit = 0.0
					max_c1profit = 0.0
					max_c2profit = 0.0
					dic_res = {}
					dic_res['league_id'] = league
					dic_res['serryname'] = serryname
					dic_res['kind'] = kind_name
					for exp in exps:
						df_exp = df_serryname.query("experiment_id==%d"%exp).sort_values(by='last_date')
						all_succ = sum(df_exp['success'])
						all_fail = sum(df_exp['failure'])
						all_profit = sum(df_exp['profit'])
						current_succ0 = sum(df_exp.tail(1)['success'])
						current_fail0 = sum(df_exp.tail(1)['failure'])
						current_profit0 = sum(df_exp.tail(1)['profit'])
						current_succ1 = sum(df_exp.tail(2)['success']) - current_succ0
						current_fail1 = sum(df_exp.tail(2)['failure']) - current_fail0
						current_profit1 = sum(df_exp.tail(2)['profit']) - current_profit0
						current_succ2 = sum(df_exp.tail(3)['success']) - current_succ0
						current_fail2 = sum(df_exp.tail(3)['failure']) - current_fail0
						current_profit2 = sum(df_exp.tail(3)['profit']) - current_profit0
						if (all_succ != 0 and float(all_fail)/float(all_succ) < min_all):
							dic_res['all_id'] = exp
							dic_res['all_fail'] = all_fail
							dic_res['all_succ'] = all_succ
							min_all = float(all_fail)/float(all_succ)
						if (current_succ2 != 0 and float(current_fail2)/float(current_succ2) < min_c2):
							dic_res['c2_id'] = exp
							dic_res['c2_fail'] = current_fail2
							dic_res['c2_succ'] = current_succ2
							min_c2 = float(current_fail2)/float(current_succ2)
						if (current_succ1 != 0 and float(current_fail1)/float(current_succ1) < min_c1):
							dic_res['c1_id'] = exp
							dic_res['c1_fail'] = current_fail1
							dic_res['c1_succ'] = current_succ1
							min_c1 = float(current_fail1)/float(current_succ1)
						if (current_succ0 != 0 and float(current_fail0)/float(current_succ0) < min_c0):
							dic_res['c0_id'] = exp
							dic_res['c0_fail'] = current_fail0
							dic_res['c0_succ'] = current_succ0
							min_c0 = float(current_fail0)/float(current_succ0)
						if (all_profit > max_allprofit):
							dic_res['all_profit'] = all_profit
							dic_res['all_p_id'] = exp
							dic_res['all_p_succ'] = all_succ
							dic_res['all_p_fail'] = all_fail
							dic_res['all_meanPorift'] = all_profit/(all_succ+all_fail)
							max_allprofit = all_profit
						if (current_profit0 > max_c0profit):
							dic_res['c0_profit'] = current_profit0
							dic_res['c0_p_id'] = exp
							dic_res['c0_p_succ'] = current_succ0
							dic_res['c0_p_fail'] = current_fail0
							dic_res['c0_meanPorift'] = current_profit0/(current_succ0+current_fail0)
							max_c0profit = current_profit0
						if (current_profit1 > max_c1profit):
							dic_res['c1_profit'] = current_profit1
							dic_res['c1_p_id'] = exp
							dic_res['c1_p_succ'] = current_succ1
							dic_res['c1_p_fail'] = current_fail1
							dic_res['c1_meanPorift'] = current_profit1/(current_succ1+current_fail1)
							max_c1profit = current_profit1
						if (current_profit2 > max_c2profit):
							dic_res['c2_profit'] = current_profit2
							dic_res['c2_p_id'] = exp
							dic_res['c2_p_succ'] = current_succ2
							dic_res['c2_p_fail'] = current_fail2
							dic_res['c2_meanPorift'] = current_profit2/(current_succ2+current_fail2)
							max_c2profit = current_profit2
					if ('all_id' not in dic_res):
						dic_res['all_id'] = 0
						dic_res['all_fail'] = 0
						dic_res['all_succ'] = 0
					if ('c2_id' not in dic_res):
						dic_res['c2_id'] = 0
						dic_res['c2_succ'] = 0
						dic_res['c2_fail'] = 0
					if ('c1_id' not in dic_res):
						dic_res['c1_id'] = 0
						dic_res['c1_succ'] = 0
						dic_res['c1_fail'] = 0
					if ('c0_id' not in dic_res):
						dic_res['c0_id'] = 0
						dic_res['c0_succ'] = 0
						dic_res['c0_fail'] = 0
					if ('all_p_id' not in dic_res):
						dic_res['all_p_id'] = 0
						dic_res['all_profit'] = 0
						dic_res['all_p_succ'] = 0
						dic_res['all_p_fail'] = 0
						dic_res['all_meanPorift'] = 0.0
					if ('c2_p_id' not in dic_res):
						dic_res['c2_p_id'] = 0
						dic_res['c2_profit'] = 0
						dic_res['c2_p_succ'] = 0
						dic_res['c2_p_fail'] = 0
						dic_res['c2_meanPorift'] = 0.0
					if ('c1_p_id' not in dic_res):
						dic_res['c1_p_id'] = 0
						dic_res['c1_profit'] = 0
						dic_res['c1_p_succ'] = 0
						dic_res['c1_p_fail'] = 0
						dic_res['c1_meanPorift'] = 0.0
					if ('c0_p_id' not in dic_res):
						dic_res['c0_p_id'] = 0
						dic_res['c0_profit'] = 0
						dic_res['c0_p_succ'] = 0
						dic_res['c0_p_fail'] = 0
						dic_res['c0_meanPorift'] = 0.0
					dic_res_str = json.dumps(dic_res,cls=GenEncoder,ensure_ascii=False)
					group_final.write(dic_res_str+'\n')
		f_kind.close()
		test_res.close()
		group_final.close()

	def getProfit(self, res_dic):
		tester = res_dic['name']
		home_team_id = res_dic['home_team']
		away_team_id = res_dic['away_team']
		date = res_dic['date']
		str_sql = "select * from Odds where home_team_id=%d and away_team_id=%d and date='%s'"%(home_team_id,away_team_id,date)
		cur.execute(str_sql)
		row = cur.fetchone()
		if row is None:
			return 0
		elif res_dic['res'] == 2:
			return -1
		res = 0
		if tester == 'MIN_GOALS_TESTER' :
			odds = row[14]
			dapan = row[13]
			if odds > 0:
				if dapan <= '2.5':
					res = odds
				elif dapan == '2.5/3':
					res = -0.5
				elif dapan == '2/2.5':
					res = 0.5 * odds
		elif tester == 'MAY_GOALS_TESTER':
			odds = row[12]
			dapan = row[13]
			if odds > 0:
				if dapan >= '2.5':
					res = odds
				elif dapan == '2.5/3':
					res = 0.5 * odds
				elif dapan == '2/2.5':
					res = -0.5
		elif tester == 'HOME_WIN_TESTER':
			odds = row[9]
			if odds > 0:
				res = odds - 1
		elif tester == 'AWAY_WIN_TESTER':
			odds = row[11]
			if odds > 0:
				res = odds - 1
		return res
