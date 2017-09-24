# -*- coding: utf-8 -*-

import json
import sqlite3
import gflags
import pandas as pd
from feature_creator import *
from filter_creator import *
from tester_creator import *
from grouper import *
from conf import *

class Processor(object):
	def __init__(self):
		global conn
		self.algs = dict()
		self.testers = dict()
		self.experiments = dict()
		f_algs = open(gflags.FLAGS.alg_path, 'r')
		data_algs = json.load(f_algs)
		str_algs = data_algs['feature']
		str_testers = data_algs['tester']
		str_filters = data_algs['filter']
		self.feature_creator = Feature_Creator(str_algs)
		self.filter_creator = Filter_Creator(str_filters)
		self.tester_creator = Tester_Creator(str_testers,self.feature_creator,self.filter_creator)
		f_experiment = open(gflags.FLAGS.experiment_path, 'r')
		data_experiment = json.load(f_experiment)
		for exp in data_experiment:
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
		f_algs.close()
		f_experiment.close()

	def process(self):
		f_buckets = open(gflags.FLAGS.buckets_path, 'r')
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
		f_buckets = open(gflags.FLAGS.buckets_path, 'r')
 		buckets = json.load(f_buckets)
		for bucket in buckets:
			experiment_id = bucket['experiment_id']
			exp = self.experiments[experiment_id]
			features = exp['feature']
			testers = exp['tester']
			filters = exp['filter']
			cands = bucket['condition']
			self.tester_creator.feature_creator.set_features(features)
			self.tester_creator.filter_creator.set_filters(filters)
			self.tester_creator.set_tester(testers)
 			for cand in cands:
				self.tester_creator.execute(cand)
		f_buckets.close()

	def group(self):
		grouper = Grouper(self.feature_creator,self.tester_creator,self.experiments)
		grouper.execute()		

	def close(self):
		global conn
		conn.close()
