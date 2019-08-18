# -*- coding: utf-8 -*-

import json
import sqlite3
import gflags
import codecs
import pandas as pd
from feature_creator import *
from filter_creator import *
from tester_creator import *
from extractor_creator import *
from predictor import *
from fix_predictor import *
from oddsgrouper import *
from testgrouper import TestGrouper
from plain_plot import *
from conf import *

class Processor(object):
	def __init__(self):
		global conn
		self.experiments = {}
		f_exp = codecs.open(gflags.FLAGS.experiment_path, 'r', encoding='utf-8')
		data_algs = json.load(f_exp)
		features = data_algs['features']
		filters = data_algs['filters']
		testers = data_algs['testers']
		experiments = data_algs['experiments']
		self.feature_creator = Feature_Creator(features)
		self.filter_creator = Filter_Creator(filters)
		self.tester_creator = Tester_Creator(testers,self.feature_creator,self.filter_creator)
		for exp in experiments:
			experiment_id = exp['experiment_id']
			algs = exp['algs']
			testers = exp['testers']
			filters = exp['filters']
			self.experiments[experiment_id] = {}
			self.experiments[experiment_id]['feature'] = []
			self.experiments[experiment_id]['tester'] = []
			self.experiments[experiment_id]['filter'] = []
			for alg in algs:
				self.experiments[experiment_id]['feature'].append(alg)
			for filter in filters:
				self.experiments[experiment_id]['filter'].append(filter)
			for tester in testers:
				self.experiments[experiment_id]['tester'].append(tester)
		f_exp.close()

	def process(self):
		pass

	def test(self):
		pass

	def group(self):
		grouper = OddsGrouper(self.tester_creator,self.experiments,gflags.FLAGS.league_cond)
		grouper.group()

	def trend_test(self):
		test_grouper = TestGrouper(self.tester_creator,self.experiments,gflags.FLAGS.league_cond)
		test_grouper.group()

	def plot(self):
		plain_plot = Plain_Plot()
		plain_plot.plot()
	
	def extract(self):
		extractor = Extractor_Creator()
		extractor.extract(gflags.FLAGS.league_cond)
	
	def predict(self):
		resfiles = gflags.FLAGS.predict_path + '*'
		os.system(r'rm -rf %s'%resfiles)
		predictor = Predictor(self.tester_creator,self.experiments)	
		predictor.predict()
		predictor.pack()	

	def predict_fix(self):
		resfiles = gflags.FLAGS.predict_path + '*'
		os.system(r'rm -rf %s'%resfiles)
		predictor = Fix_Predictor(self.tester_creator)	
		predictor.predict()
		predictor.pack()	

	def close(self):
		global conn
		conn.close()
