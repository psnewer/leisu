# -*- coding: utf-8 -*-
import codecs
from conf import *
from feature_creator import Feature_Creator
from filter_creator import Filter_Creator
from tester_creator import Tester_Creator
from extractor_creator import Extractor_Creator
from analysis_creator import Analysis_Creator
from predictor import Predictor

class Processor(object):
	def __init__(self):
		global conn
		# self.experiments = {}
		# f_exp = codecs.open(gflags.FLAGS.experiment_path, 'r', encoding='utf-8')
		# data_algs = json.load(f_exp)
		# features = data_algs['features']
		# filters = data_algs['filters']
		# testers = data_algs['testers']
		# experiments = data_algs['experiments']
		# self.feature_creator = Feature_Creator()
		# self.filter_creator = Filter_Creator()
		# self.tester_creator = Tester_Creator()
		# for exp in experiments:
		# 	experiment_id = exp['experiment_id']
		# 	algs = exp['algs']
		# 	testers = exp['testers']
		# 	filters = exp['filters']
		# 	self.experiments[experiment_id] = {}
		# 	self.experiments[experiment_id]['feature'] = []
		# 	self.experiments[experiment_id]['tester'] = []
		# 	self.experiments[experiment_id]['filter'] = []
		# 	for alg in algs:
		# 		self.experiments[experiment_id]['feature'].append(alg)
		# 	for filter in filters:
		# 		self.experiments[experiment_id]['filter'].append(filter)
		# 	for tester in testers:
		# 		self.experiments[experiment_id]['tester'].append(tester)
		# f_exp.close()

	def process(self):
		pass

	def test(self):
		pass
	
	def extract(self):
		extractor = Extractor_Creator()
		if 'league_cond' not in gflags.FLAGS:
			f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
			league_conds = json.load(f_exp)
			for league_cond in league_conds:
				extractor.execute(league_cond)
		else:
			extractor.execute(json.loads(gflags.FLAGS.league_cond))

	def feature(self):
		featurer = Feature_Creator()
		if 'league_cond' not in gflags.FLAGS:
			f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
			league_conds = json.load(f_exp)
			for league_cond in league_conds:
				featurer.execute(league_cond)
		else:
			featurer.execute(json.loads(gflags.FLAGS.league_cond))

	def filter(self):
		filter = Filter_Creator()
		if 'league_cond' not in gflags.FLAGS:
			f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
			league_conds = json.load(f_exp)
			for league_cond in league_conds:
				filter.execute(league_cond)
		else:
			filter.execute(json.loads(gflags.FLAGS.league_cond))

	def test(self):
		tester = Tester_Creator()
		if 'league_cond' not in gflags.FLAGS:
			f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
			league_conds = json.load(f_exp)
			for league_cond in league_conds:
				tester.execute(league_cond)
		else:
			tester.execute(json.loads(gflags.FLAGS.league_cond))
	
	def analysis(self):
		analysis = Analysis_Creator()
		if 'league_cond' not in gflags.FLAGS:
			f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
			league_conds = json.load(f_exp)
			for league_cond in league_conds:
				analysis.execute(league_cond)
		else:
			analysis.execute(json.loads(gflags.FLAGS.league_cond))
	
	def predict(self):
		predictor = Predictor()	
		predictor.predict()	

	def close(self):
		global conn
		conn.close()
