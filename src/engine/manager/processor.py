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
from grouper import *
from predictor import *
from oddsgrouper import *
from groupprocessor import *
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
		f_buckets = codecs.open(gflags.FLAGS.buckets_path, 'r', encoding='utf-8')
		buckets = json.load(f_buckets)
		for bucket in buckets:
			experiment_id = bucket['experiment_id']
			exp = self.experiments[experiment_id]
			features = exp['feature']
			filters = exp['filter']
			testers = exp['tester']
			cands = bucket['condition']
			self.feature_creator.set_features(features)
			self.filter_creator.set_features(filters)
			for cand in cands:
				self.feature_creator.execute(cand)
		f_buckets.close()

	def test(self):
		f_buckets = codecs.open(gflags.FLAGS.buckets_path, 'r', encoding='utf-8')
		buckets = json.load(f_buckets)
		for bucket in buckets:
			experiment_id = bucket['experiment_id']
			exp = self.experiments[experiment_id]
			testers = exp['tester']
			filters = exp['filter']
			cands = bucket['condition']
			for cand in cands:
				self.tester_creator.execute(cand)
				self.tester_creator.test(filters,testers,cand)
		f_buckets.close()

	def group(self):
			grouper = OddsGrouper(self.tester_creator,self.experiments,gflags.FLAGS.league_cond)
			grouper.group()

	def extract(self):
		extractor = Extractor_Creator()
		extractor.extract(gflags.FLAGS.league_cond)
	
	def predict(self):
		resfiles = gflags.FLAGS.predict_path + '*'
		os.system(r'rm -rf %s'%resfiles)
		predictor = Predictor(self.tester_creator,self.experiments)	
		predictor.predict()
		predictor.pack()	

	def close(self):
		global conn
		conn.close()
