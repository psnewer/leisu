# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import gflags
import codecs
import matplotlib.pyplot as plt
from conf import *

class Plain_Plot():
	def __init__(self):
		pass

	def plot(self):
		walks = os.walk('/Users/miller/Documents/workspace/leisu/res/group',topdown=False)
		for root,dirs,files in walks:
			if 'test_res.txt' in files and root=='/Users/miller/Documents/workspace/leisu/res/group/168/default':
				test_res_file = os.path.join(root,'test_res.txt')
				df_test = pd.DataFrame.from_csv(test_res_file).astype({'serryid': str})
				dic_res = {}
				serryids = df_test['serryid'].unique()
				if len(df_test) == 0:
					continue
				league_id = df_test.iloc[-1]['league_id']
				serryname = df_test.iloc[-1]['serryname']
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
							dic_res[experiment_id][pre] = df_experiment[df_experiment['serryid']==_serryid].to_dict('record')
						else:
							dic_res[experiment_id][pre] = []
				self.execute_plot(root,dic_res)

	def execute_plot(self,dir,dic_res):
		os.system(r'mkdir %s'%(dir+'/plot'))
		for exp_id in dic_res:
			exp_dic = dic_res[exp_id]
			length = len(exp_dic)
			fig = plt.figure()
			fig.set_size_inches(20,50)
			for idx,pre in enumerate(sorted(exp_dic.keys())):
				team_res = 	exp_dic[pre]
				if team_res == []:
					continue
				df = pd.DataFrame(team_res).sort_values(by='date')
				dates =df['date'].unique()
				res = []
				for date in dates:
					df_date = df[df.date <= date]
					succ = 0
					fail = 0
					neu = 0
					per_count = df_date['res'].value_counts()
					if 0 in per_count:
						neu = per_count[0]
					if 1 in per_count:
						succ = per_count[1]
					if 2 in per_count:
						fail = per_count[2]
					limi = 0.0	
					if gflags.FLAGS.with_neu and succ > 0:
						limi = float(succ + fail + neu)/float(succ)
					elif succ > 0:
						limi = float(succ + fail)/float(succ)
					if limi > 0.0:
						res.append(limi)
				ax = fig.add_subplot(length,1,idx+1)
				X = range(0,len(res))
				Y = res
				ax.plot(X,Y)
				plt.title(pre)
			plt.savefig(dir + '/plot/' + '/' + str(exp_id) + '.png')
			plt.close(fig)


