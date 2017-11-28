# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import gflags
import codecs
from conf import *
from vs_plainself_extractor import *
from vs_plainself_no_pre_extractor import *
from vs_plainto_extractor import *
from vs_plainall_extractor import *
from vs_rawself_extractor import *
from vs_rawall_extractor import *
from goal_rawall_extractor import *
from vs_rawall_no_pre_extractor import *

class Extractor_Creator(object):
	def __init__(self):
		self.extractors = {}
		self.extractor_cand = []
		f_exp = codecs.open(gflags.FLAGS.extract_conf, 'r', encoding='utf-8')
		data_features = json.load(f_exp)
		features = data_features['features']
		experiments = data_features['experiments']
		for feature in features:
			flag = feature['flag']
			if flag in experiments:
				featurestr = feature['name'] + '()'
				extractor_ins = eval(featurestr)
				extractor_ins.setParams(feature['params'])
				self.extractor_cand.append(extractor_ins)

	def extract(self,condition):
		condition = json.loads(condition)
		league_str = str(condition['league_id'])
		league_cond = 'league_id=' + league_str
		self.extract_directory = gflags.FLAGS.extract_path + league_str
		os.system(r'mkdir %s'%self.extract_directory)
		for serryname in condition['serryname']:
			os.system(r"mkdir '%s'"%(self.extract_directory+'/'+serryname))
			serry_cond = "serryname='%s'"%serryname
			cond = [league_cond,serry_cond]
			for extractor in self.extractor_cand:
				extract_train_txt = self.extract_directory + '/' + serryname + '/' + extractor.name + '_' + 'extract_train.txt'
				extract_test_txt = self.extract_directory + '/' + serryname + '/' +extractor.name + '_' + 'extract_test.txt'
				extract_train = self.extract_directory + '/' + serryname + '/' + extractor.name + '_' + 'extract_train.csv'
				extract_test = self.extract_directory + '/' + serryname + '/' + extractor.name + '_' + 'extract_test.csv'
				extractor.process(cond,extract_train_txt,extract_test_txt,extract_train,extract_test)
		
