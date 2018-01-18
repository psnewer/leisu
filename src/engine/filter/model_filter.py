#!/bin/bash
# -*- coding: utf-8 -*-
import sys
sys.path.append('/Users/miller/Documents/workspace/caffe/python')
import numpy as np
import caffe
from conf import *
import pandas as pd
import sqlite3
import gflags
import codecs
import json
from abstract_filter import *

class MODEL_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'MODEL_FILTER'
		self.params = {}
		self.params['area'] = 1
		self.params['ignore'] = True
		self.params['tar'] = 0
		self.params['proto'] = ''
		self.params['weights'] = ''
		self.params['feature'] = ''
#		self.params['x_rt'] = False
#		self.params['x_rt_feature'] = ''
#		self.params['x_rt_thresh'] = 0.5
#		rt_data = codecs.open('/Users/miller/Documents/workspace/leisu/ZOO/goal_rt.json','r+',encoding='utf-8')
#		self.rt = json.load(rt_data)
		caffe.set_mode_cpu()
		self.net = None

	def setParams(self,params):
		super(MODEL_FILTER,self).setParams(params)
		if gflags.FLAGS.group or gflags.FLAGS.predict:
			self.net = caffe.Net(str(self.params['proto']), str(self.params['weights']), caffe.TEST)

	def filter(self,df):
		delete_row = []
#		if len(df) > 0 and self.params['x_rt']:
#			rowlast = df.iloc[-1]
#			date = rowlast['date']
#			team_id = rowlast['team_id']
#			area = rowlast['area']
#			sql_str = ''
#			match = ''
#			if gflags.FLAGS.predict:
#				match = 'TMatch'
#			else:
#				match = 'Match'
#			if area == 1:
#				sql_str = "select league_id,serryname from %s where home_team_id=%d and date like '%s'"%(match,team_id,date+'%')
#			else:
#				sql_str = "select league_id,serryname from %s where away_team_id=%d and date like '%s'"%(match,team_id,date+'%')
#			df_serry = pd.read_sql_query(sql_str,conn)
#			league_id = df_serry.iloc[-1]['league_id']
#			serryname = df_serry.iloc[-1]['serryname']
#			if not (str(league_id) in self.rt and serryname in self.rt[str(league_id)] and  self.params['x_rt_feature'] in self.rt[str(league_id)][serryname] and self.rt[str(league_id)][serryname][self.params['x_rt_feature']] > self.params['x_rt_thresh']):
#				for i in range(0,len(df)):
#					delete_row.append(i)
#				return delete_row
		for idx,row in df.iterrows():
			if row['area'] != self.params['area'] and self.params['area'] > 0:
				if self.params['ignore']:
					continue
				else:
					delete_row.append(idx)
					continue
			team = row['team_id']
			to_team = row['toteam']
			date = row['date']
			row_to = df.query("team_id==%d & date=='%s'"%(to_team,date)).iloc[-1]
			if self.params['feature'] not in row or self.params['feature'] not in row_to:
				delete_row.append(idx)
				continue
			home_res = row[self.params['feature']]
			away_res = row_to[self.params['feature']]
			if len(home_res)==0 or len(away_res)==0:
				delete_row.append(idx)
				continue
			self.net.blobs['home_res'].data[...] = np.array(home_res)
			self.net.blobs['away_res'].data[...] = np.array(away_res)
			self.net.forward()	
			res = self.net.blobs['softmax'].data[0]
			label = res.argmax(axis=0)
			if (label != self.params['tar']):
				delete_row.append(idx)
		return delete_row
