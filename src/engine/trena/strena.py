# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('.')
import json
import gflags
import sqlite3
import os
import re
import copy
import collections 
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import codecs
import operator
import string
from dateutil.parser import parse
from conf import *

kind_data = codecs.open('./conf/kinds.txt','r',encoding='utf-8')
kind_list = json.load(kind_data)
kind_data.close()

bs_data = codecs.open('./conf/bs.txt','r',encoding='utf-8')
bs_list = json.load(bs_data)
_test_ids = bs_list['sell']
bs_data.close()

f_exp = codecs.open('./conf/strena.conf','r',encoding='utf-8')
experiments = json.load(f_exp)
sexperiments = {}
for exp in experiments:
	flag = exp['flag']
	params = exp['params']
	kinds = exp['kind']
	exps = []
	for kind in kinds:
		exps.extend(kind_list[kind])
	sexperiments[flag] = {}
	sexperiments[flag]['exps'] = exps
	sexperiments[flag]['params'] = params
f_exp.close()

walks = os.walk('../../res/group',topdown=False)
for root,dirs,files in walks:
	if 'trent_res.txt' in files:
		trent_file = os.path.join(root,'trent_res.txt')
		df_trent = pd.DataFrame.from_csv(trent_file)
		if len(df_trent) == 0:
			continue
		res = {}
		for flag in sorted(sexperiments.keys()):
			res[flag] = {}
			res[flag]['te'] = {}
			res[flag]['limi_odds'] = sexperiments[flag]['params']['thresh_odds']
		test_ids = df_trent['test_id'].unique()
		for test_id in test_ids:
			if test_id not in _test_ids:
				continue
			df_testid = df_trent[df_trent['test_id']==test_id]
			experiment_ids = df_testid['experiment_id'].unique()
			for experiment_id in experiment_ids:
				for flag in sorted(sexperiments.keys()):
					exps = sexperiments[flag]['exps']
					params = sexperiments[flag]['params']
					if experiment_id not in exps:
						continue
					posi = 0
					neg = 0
					df_experiment = df_testid[df_testid['experiment_id']==experiment_id]
					pres = df_experiment['pre'].unique()
					pres.sort()
					for pre in pres:
						row_pre = df_experiment[df_experiment['pre']==pre].iloc[0]
						if pre==0 and row_pre['limi_odds'] < params['thresh_odds']:
							break
						df_pres = df_experiment[df_experiment['pre'] > pre].head(params['num'])
						if len(df_pres) < params['min_num'] or pre==0:
							continue
						num_neg = len(df_pres[df_pres['limi_odds'] >= params['neg_odds']])
						num_posi = len(df_pres) - num_neg
						if float(num_posi) / float(num_posi + num_neg) >= params['posi_rt']:
							if row_pre['limi_odds'] >= params['posi_odds']:
								neg += 1
							else:
								posi += 1
					if posi > params['posi_num'] and float(posi) / float(posi + neg) > params['trena_rt']:
						if test_id in res[flag]['te']:
							res[flag]['te'][test_id].append(experiment_id)
						else:
							res[flag]['te'][test_id] = [experiment_id]
						break
		test_final = codecs.open(os.path.join(root,'stest_final.txt'),'w+',encoding='utf-8')
		json.dump(res, test_final, ensure_ascii=False)
		test_final.close()
						
			

	

