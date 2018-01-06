# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import gflags
import codecs
import threading
from trend_creator import *
from trenf_creator import *
from trent_creator import *
from conf import *

class TestGrouper():
	def __init__(self,tester_creator,experiments,condition):
		self.tester_creator = tester_creator
		self.experiments = experiments
		self.trend_experiments = {}
		f_exp = codecs.open(gflags.FLAGS.trend_path, 'r', encoding='utf-8')
		data_algs = json.load(f_exp)
		f_exp.close()
		features = data_algs['features']
		filters = data_algs['filters']
		self.trend_creator = Trend_Creator(features)
		self.trenf_creator = Trenf_Creator(filters)
		self.trent_creator = Trent_Creator(self.trend_creator,self.trenf_creator)
		trend_experiments = data_algs['experiments']
		for exp in trend_experiments:
			experiment_id = exp['experiment_id']
			algs = exp['algs']
			filters = exp['filters']
			self.trend_experiments[experiment_id] = {}
			self.trend_experiments[experiment_id]['feature'] = []
			self.trend_experiments[experiment_id]['filter'] = []
			for alg in algs:
				self.trend_experiments[experiment_id]['feature'].append(alg)
			for filter in filters:
				self.trend_experiments[experiment_id]['filter'].append(filter)
		condition = json.loads(condition)
		league_str = str(condition['league_id'])
		self.conditions = []
		league_cond = 'league_id=' + league_str
		for serryname in condition['serryname']:
			serry_cond = "serryname='%s'"%serryname
			self.conditions.append([league_cond,serry_cond])
		self.group_directory = gflags.FLAGS.group_path + league_str
		os.system(r'mkdir %s'%self.group_directory)

	def group(self):
#		self.process()
		self.analysis()

	def process(self):
		for cond in self.conditions:
			league_id = int(cond[0].split('=')[1])
			serryname = cond[1].split('=')[1].strip('\'')
			group_directory = self.group_directory + '/' + serryname
			os.system(r'mkdir %s'%group_directory)
			feature_res = group_directory + '/feature_res.txt'
			test_res = group_directory + '/test_res.txt'
			feature_res = codecs.open(feature_res,'w+',encoding='utf-8')
			test_res = codecs.open(test_res,'w+',encoding='utf-8')
			self.tester_creator.group(cond,feature_log=feature_res)
			df = pd.DataFrame([])
			for experiment_id in self.experiments:
				exp = self.experiments[experiment_id]
				features = exp['feature']
				filters = exp['filter']
				testers = exp['tester']
				team_res = self.tester_creator.test(filters,testers,cond)
				df_res = pd.DataFrame(team_res)
				df_res['experiment_id'] = experiment_id
				df_res['league_id'] = league_id
				df_res['serryname'] = serryname
				df = df.append(df_res)
			df.to_csv(test_res)
			feature_res.close()
			test_res.close()

	def analysis(self):
		for cond in self.conditions:
			league_id = int(cond[0].split('=')[1])
			serryname = cond[1].split('=')[1].strip('\'')
			group_directory = self.group_directory + '/' + serryname
			test_res_file = group_directory + '/test_res.txt'
			trend_res_file = group_directory + '/trend_res.txt'
			test_final_file = group_directory + '/test_final.txt'
			trend_final_file = group_directory + '/trend_final.txt'
			if os.path.exists(test_res_file):
				df_test = pd.DataFrame.from_csv(test_res_file)
				if len(df_test) == 0:
					continue
				df_test = df_test.astype({'serryid': str, "date": str})
				dic_res = {}
				serryids = df_test['serryid'].unique()
				sql_str = "select distinct serryid from Match where league_id=%d and serryname='%s' order by date desc"%(league_id,serryname)
				_serryids = pd.read_sql_query(sql_str,conn)
				started = True
				first_serryid = _serryids.iloc[0][0]
				if not gflags.FLAGS.test_all:
					sql_str = "select count(*) from TMatch where league_id=%d and serryid='%s'"%(league_id,first_serryid)
					count = pd.read_sql_query(sql_str,conn).iloc[0][0]
					if count == 0:
						started = False	
				experiment_ids = df_test['experiment_id'].unique()
				for experiment_id in experiment_ids:
					df_experiment = df_test[df_test['experiment_id']==experiment_id]
					dic_res[experiment_id] = {}
					for idx,row in _serryids.iterrows():
						_serryid = row['serryid']
						pre = idx
						if not started:
							pre = pre + 1
						if _serryid in serryids:
							pre = idx
							if not started:
								pre = pre + 1
							dic_res[experiment_id][pre] = df_experiment[df_experiment['serryid']==_serryid].query("date < '%s'"%gflags.FLAGS.date_thresh).to_dict('record')
						else:
							dic_res[experiment_id][pre] = []
				if gflags.FLAGS.test:
					trend_res = codecs.open(trend_res_file,'w+',encoding='utf-8')
					self.trent_creator.execute(dic_res,trend_log=trend_res)
					team_res = []
					kind_trend_file = codecs.open(gflags.FLAGS.kind_trend,'r',encoding='utf-8')
					kind_trend = json.load(kind_trend_file)
					kind_ids = {}
					df_tmp = pd.DataFrame([])
					for row in kind_trend:
						trend_experiment = row['trend_experiment']
						experiments = row['experiment']
						trend_ids = []
						exp = self.trend_experiments[trend_experiment]
						features = exp['feature']
						filters = exp['filter']
						trent_res = self.trent_creator.test(filters,dic_res,test_id=trend_experiment)
						df_trent = pd.DataFrame(trent_res)
						df_tmp = df_tmp.append(df_trent)
						for experiment_id in experiments:
							if len(df_trent) > 0 and self.filter_trend(df_trent, experiment_id):
								trend_ids.append(experiment_id)
						if trend_ids:
							kind_ids[trend_experiment] = trend_ids
					df_tmp.to_csv(group_directory + '/trent_res.txt')
					test_final = codecs.open(test_final_file,'w+',encoding='utf-8')
					json.dump(kind_ids, test_final, ensure_ascii=False)
					trend_res.close()
					test_final.close()
					kind_trend_file.close()
				thresh_pre = self.get_threshPre(league_id,serryname)
				kind_trend_file = codecs.open(gflags.FLAGS.kind_trend,'r',encoding='utf-8')
				kind_trend = json.load(kind_trend_file)
				kind_trend_file.close()
				kind_ids = {}
				if os.path.exists(group_directory + '/trent_res.txt'):
					df_trent = pd.DataFrame.from_csv(group_directory + '/trent_res.txt')
					if len(df_trent) > 0:
						for row in kind_trend:
							trend_experiment = row['trend_experiment']
							experiments = row['experiment']
							trend_ids = []
							exp = self.trend_experiments[trend_experiment]
							df_te = df_trent[df_trent['test_id']==trend_experiment]
							for experiment_id in experiments:
								if len(df_trent) > 0 and self.filter_trend(df_te, experiment_id, thresh_pre):
									trend_ids.append(experiment_id)
							if trend_ids:
								kind_ids[trend_experiment] = trend_ids
				test_final = codecs.open(test_final_file,'w+',encoding='utf-8')
				json.dump(kind_ids, test_final, ensure_ascii=False)
				test_final.close()
				trend_res = codecs.open(trend_res_file,'w+',encoding='utf-8')
				self.trent_creator.predict(dic_res,trend_log=trend_res)
				test_final = codecs.open(test_final_file,'r',encoding='utf-8')
				trend_final = codecs.open(trend_final_file,'w+',encoding='utf-8')
				trend_dic = json.load(test_final)
				filtered_exps = {}
				for key in trend_dic:
					test_id = int(key) 
					experiment_ids = trend_dic[key]
					exp = self.trend_experiments[test_id]
					features = exp['feature']
					filters = exp['filter']
					trenf_res = self.trent_creator.get_filtered(filters)
					if trenf_res is not None and len(trenf_res) > 0:
						filtered_experiments = trenf_res['experiment_id'].unique().tolist()
						for experiment_id in experiment_ids:
							if experiment_id in filtered_experiments:
								if experiment_id not in filtered_exps:
									experiment_dic = dic_res[experiment_id]
									filtered_exps[experiment_id] = {}
									filtered_exps[experiment_id]['limi'] = self.getLimi(experiment_dic)
									filtered_exps[experiment_id]['test_id'] = [test_id]
								else:
									filtered_exps[experiment_id]['test_id'].append(test_id)
				json.dump(filtered_exps,trend_final)
				self.trent_creator.filter_experiment()
				trend_res.close()
				test_final.close()
				trend_final.close()

	def filter_trend(self, df_trent, experiment_id, thresh_pre):
		num_posi = 0
		num_neg = 0
		df_experiment = df_trent[df_trent['experiment_id']==experiment_id]
		for idx,row in df_experiment.iterrows():
			if row['pre'] <= thresh_pre:
				continue
			limi_odds = row['limi_odds']
			limi_odds_with_neu = row['limi_odds_with_neu']
			if limi_odds > 1.34:
				num_neg += 1
			else:
				num_posi += 1
		if num_posi + num_neg < 4:
			return False
		if num_posi > 0 and float(num_posi) / float(num_posi + num_neg) >= 0.7:
			return True
		return False  

	def getLimi(self, experiment_dic):
		res = {}
		if 0 in experiment_dic and experiment_dic[0]:
			pre_detail = experiment_dic[0]
			df_team = pd.DataFrame(pre_detail)
			per_counts = df_team['res'].value_counts()
			succ = 0
			fail = 0
			neu = 0
			if 0 in per_counts:
				neu += per_counts[0]
			if 1 in per_counts:
				succ += per_counts[1]
			if 2 in per_counts:
				fail += per_counts[2]
			if succ > 0:
				res['limi'] = float(succ + fail) / float(succ)
				res['limi_with_neu'] = float(succ + fail + neu) / float(succ)
			else:
				res['limi'] = 100.0
				res['limi_with_neu'] = 100.0
		return res

	def get_threshPre(self,league_id,serryname):
		date = gflags.FLAGS.date_thresh
		cur.execute("select distinct serryid from Match_back where league_id=%d and serryname='%s' order by date desc"%(league_id,serryname))
		all_serryids = cur.fetchall()
		cur.execute("select distinct serryid from Match where league_id=%d and serryname='%s' and date < '%s' order by date desc"%(league_id,serryname,gflags.FLAGS.date_thresh))
		serryid = cur.fetchall()[0][0]
		for idx,ele in enumerate(all_serryids):
			if serryid == ele[0]:
				return idx
		return -1	
		
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
		elif res_dic['res'] == 2 or res_dic['res'] == 0:
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
