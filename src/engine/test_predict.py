# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import sqlite3
import os
import re
import xml.etree.ElementTree as ET
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

def analysis(df_team,df_tmatch,tester):
	anadf = pd.merge(df_team,df_tmatch,how='inner',on=['home_team_id','away_team_id'])
	num_posi = 0
	num_neg = 0
	num_neu = 0
	for idx,row in anadf.iterrows():
		if tester == 'min_goal':
			if row['home_goal'] + row['away_goal'] < 2.5:
				num_posi += 1
			elif row['home_goal'] + row['away_goal'] > 2.5:
				num_neg += 1
		elif tester == 'may_goal':
			if row['home_goal'] + row['away_goal'] < 2.5:
				num_neg += 1
			elif row['home_goal'] + row['away_goal'] > 2.5:
				num_posi += 1
	return num_posi,num_neg,num_neu

predict_detail = codecs.open('../res/predict_detail.txt','a+',encoding='utf-8')
filter_trend_file = codecs.open('./conf/filter_trend.conf','r',encoding='utf-8')
filter_trend = json.load(filter_trend_file)
df_filter = pd.DataFrame(filter_trend)
filter_trend_file.close()
walks = os.walk('../res/predict',topdown=False)
team_res = []
for root,dirs,files in walks:
	if 'predict_res.txt' in files:
		predict_res_file = os.path.join(root,'predict_res.txt')
		predictdata = codecs.open(predict_res_file,'r',encoding='utf-8')
		df_predict = pd.DataFrame([])
		league_id = 0
		for row in predictdata:
			predict_dic = json.loads(row,encoding='utf-8')
			league_id = predict_dic['league_id']
			league = cur.execute("select name from League where id=%d"%league_id).fetchone()[0]
			serryname = predict_dic['serryname']
			tester = predict_dic['tester']
			exp_id = predict_dic['exp_id']
			sql_str = "select league_id,home_team_id,away_team_id,home_goal,away_goal from TMatch where league_id=%d"%(league_id)
			df_tmatch = pd.read_sql_query(sql_str,conn)
			if len(df_filter[(df_filter['league']==league) & (df_filter['tester']==tester)]) > 0:
				continue
			if len(predict_dic['res']) > 0:
				df_ele = pd.DataFrame(predict_dic['res']).sort_values(by='date')
				date = df_ele.iloc[0]['date']
				df_ele = df_ele[df_ele['date']==date]
				num_posi,num_neg,num_neu = analysis(df_ele,df_tmatch,tester)
				res = {}
				res['league_id'] = league_id
				res['serryname'] = serryname
				res['tester'] = tester
				res['exp_id'] = exp_id
				res['posi'] = num_posi
				res['neg'] = num_neg
				res['neu'] = num_neu
				res['res'] = predict_dic['res']
				res_str = json.dumps(res,ensure_ascii=False,encoding='utf-8')
				predict_detail.write(res_str+'\n')
				df_predict = df_predict.append(df_ele)
		if len(df_predict) > 0:
			df_predict.drop_duplicates(['date','home_team_id','away_team_id'],inplace=True)
			num_posi,num_neg,num_neu = analysis(df_predict,df_tmatch,tester)
			res ={}
			res['league_id'] = league_id
			res['serryname'] = serryname
			res['posi'] = num_posi
			res['neg'] = num_neg
			res['neu'] = num_neu
			team_res.append(res)
if team_res:
	per_counts = pd.DataFrame(team_res)
	posi = sum(per_counts['posi'])
	neg = sum(per_counts['neg'])
	neu = sum(per_counts['neu'])
	print posi,neg,neu
predict_detail.close()

