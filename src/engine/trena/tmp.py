# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/Users/miller/Documents/workspace/leisu/src/engine')
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

f_exp = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/trena.conf','r',encoding='utf-8')
experiments = json.load(f_exp)
sexperiments = {}
for exp in experiments:
	flag = exp['flag']
	params = exp['params']
	sexperiments[flag] = params
f_exp.close()

filter_trend_file = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/filter_trend.conf','r',encoding='utf-8')
filter_trend = json.load(filter_trend_file)
df_filter = pd.DataFrame(filter_trend)
filter_trend_file.close()

pre_thresh = 1 
result_posi = 0
result_neg = 0
walks = os.walk('/Users/miller/Documents/workspace/leisu/res/group',topdown=False)
for root,dirs,files in walks:
	if 'trent_res.txt' in files:
		matchObj = re.match( r'.*/(.*)/(.*)$', root)
		league_id = matchObj.group(1)
		serryname = matchObj.group(2)
		league = cur.execute("select name from League where id=%d"%int(league_id)).fetchone()[0]
		if len(df_filter[(df_filter['league']==league)]) > 0:
			continue
		trent_file = os.path.join(root,'trent_res.txt')
		df_trent = pd.DataFrame.from_csv(trent_file)
		if len(df_trent) == 0:
			continue
		res = {}
		for flag in sexperiments:
			params = sexperiments[flag]
			test_ids = df_trent['test_id'].unique()
			res_ele = {}
			posi_pre_2 = 0
			neg_pre_2 = 0
			posi_f = 0
			neg_f = 0
			posi_pre_1 = 0
			neg_pre_1 = 0
			ele = {}
			for test_id in test_ids:
				df_testid = df_trent[df_trent['test_id']==test_id]
				experiment_ids = df_testid['experiment_id'].unique()
				for experiment_id in experiment_ids:
					if experiment_id < 28:
						continue
					posi = 0
					neg = 0
					posi_pre = 0
					neg_pre = 0
					df_experiment = df_testid[df_testid['experiment_id']==experiment_id]
					pres = df_experiment['pre'].unique()
					for pre in pres:
						if pre == 0:
							continue
						row_pre = df_experiment[df_experiment['pre']==pre].iloc[0]
						df_pres = df_experiment[df_experiment['pre'] > pre]
						if len(df_pres) < params['num']:
							continue
						num_neg = len(df_pres[df_pres['limi_odds'] > params['neg_odds']])
						num_posi = len(df_pres) - num_neg
						if float(num_posi) / float(num_posi + num_neg) >= params['posi_rt']:
							if row_pre['limi_odds'] > params['posi_odds']:
								if pre > pre_thresh:
									neg += 1
									neg_f += 1
								else:
									neg_pre += 1
							else:
								if pre > pre_thresh:
									posi += 1
									posi_f += 1
								else: 
									posi_pre += 1
					if posi > 0 and float(posi) / float(posi + neg) >= params['trena_rt']:
						if test_id in ele:
							ele[test_id].append(experiment_id)
						else:
							ele[test_id] = [experiment_id]
						posi_pre_1 += posi_pre
						neg_pre_1 += neg_pre
			if ele and posi_f > 0 and float(posi_f) / float(posi_f + neg_f) >= params['f_rt']:
				for _test_id in ele:
					for _experiment_id in ele[_test_id]:
						if _test_id in res_ele:
							res_ele[_test_id].append(_experiment_id)
						else:
							res_ele[_test_id] = [_experiment_id]
				posi_pre_2 += posi_pre_1
				neg_pre_2 += neg_pre_1
			if res_ele:
				res[flag] = {}
				res[flag]['te'] = res_ele
				res[flag]['limi_odds'] = params['posi_odds']
				result_posi += posi_pre_2
				result_neg += neg_pre_2
				print posi_pre_2,neg_pre_2,league
print result_posi,result_neg
#		test_final = codecs.open(os.path.join(root,'test_final.txt'),'w+',encoding='utf-8')
#		json.dump(res, test_final, ensure_ascii=False)
#		test_final.close()
						
			

	
