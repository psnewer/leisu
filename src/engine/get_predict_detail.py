# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
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

team_res = []
predict_detail = codecs.open('../res/predict_tmp.txt','r',encoding='utf-8')
for row in predict_detail:
	ele = json.loads(row)
	test_ids = ele['test_id']
	for test_id in test_ids:
		predict_detail_ele = copy.deepcopy(ele)
		predict_detail_ele['test_id'] = test_id
		team_res.append(predict_detail_ele)
df = pd.DataFrame(team_res)
#df.to_csv('../res/predict_detail.csv',encoding="utf_8_sig")
predict_detail.close()

f1 = lambda x: float(x['posi']['sum']+x['neg']['sum'])/float(x['posi']['sum']) if x['posi']['sum']>0 else 0.0
f2 = lambda x: x['posi']['sum'] + x['neg']['sum']
df_league = df[['league_id','serryname','tester','posi','neg','neu']].groupby(['league_id','serryname','tester']).agg(['sum']).reset_index()
df_league['league_accuray'] = df_league.apply(f1 ,axis=1)
df_league['league_count'] = df_league.apply(f2, axis=1)
df_league.rename(columns={'posi':'league_posi','neg':'league_neg','neu':'league_neu'},inplace=True)
df_trend = df[['league_id','serryname','tester','test_id','posi','neg','neu']].groupby(['league_id','serryname','tester','test_id']).agg(['sum']).reset_index()
df_trend['trend_accuray'] = df_trend.apply(f1 ,axis=1)
df_trend['trend_count'] = df_trend.apply(f2, axis=1)
df_trend.rename(columns={'posi':'trend_posi','neg':'trend_neg','neu':'trend_neu'},inplace=True)
df_exp = df[['league_id','serryname','tester','exp_id','posi','neg','neu']].groupby(['league_id','serryname','tester','exp_id']).agg(['sum']).reset_index()
df_exp['exp_accuray'] = df_exp.apply(f1, axis=1)
df_exp['exp_count'] = df_exp.apply(f2, axis=1)
df_exp.rename(columns={'posi':'exp_posi','neg':'exp_neg','neu':'exp_neu'},inplace=True)
df_te = df[['league_id','serryname','tester','test_id','exp_id','posi','neg','neu']].groupby(['league_id','serryname','tester','test_id','exp_id']).agg(['sum']).reset_index()
df_te['te_accuray'] = df_te.apply(f1, axis=1)
df_te['te_count'] = df_te.apply(f2, axis=1)
df_te.rename(columns={'posi':'te_posi','neg':'te_neg','neu':'te_neu'},inplace=True)

df_detail = df_te.merge(df_exp,how='inner',on=['league_id','serryname','tester','exp_id'])
df_detail = df_detail.merge(df_trend,how='inner',on=['league_id','serryname','tester','test_id'])
df_detail = df_detail.merge(df_league,how='inner',on=['league_id','serryname','tester'])
df_detail.to_csv('../res/predict_detail.csv')
