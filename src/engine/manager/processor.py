# -*- coding: utf-8 -*-
import codecs
from conf import *
from extractor.soccer.extractor_creator import Extractor_Creator
from extractor.tennis.ten_extractor_creator import Ten_Extractor_Creator
from feature.soccer.feature_creator import Feature_Creator
from feature.tennis.ten_feature_creator import Ten_Feature_Creator
from filter.soccer.filter_creator import Filter_Creator
from filter.tennis.ten_filter_creator import Ten_Filter_Creator
from tester.soccer.tester_creator import Tester_Creator
from tester.tennis.ten_tester_creator import Ten_Tester_Creator
from analysis.soccer.analysis_creator import Analysis_Creator
from analysis.tennis.ten_analysis_creator import Ten_Analysis_Creator
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
		if gflags.FLAGS.soccer:
			extractor = Extractor_Creator()
			if 'league_cond' not in gflags.FLAGS:
				f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
				league_conds = json.load(f_exp)
				for league_cond in league_conds:
					extractor.execute(league_cond)
			else:
				extractor.execute(json.loads(gflags.FLAGS.league_cond))
		else:
			extractor = Ten_Extractor_Creator()
			extractor.execute()

	def feature(self):
		if gflags.FLAGS.soccer:
			featurer = Feature_Creator()
			if 'league_cond' not in gflags.FLAGS:
				f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
				league_conds = json.load(f_exp)
				for league_cond in league_conds:
					featurer.execute(league_cond)
			else:
				featurer.execute(json.loads(gflags.FLAGS.league_cond))
		else:
			featurer = Ten_Feature_Creator()
			featurer.execute()

	def filter(self):
		if gflags.FLAGS.soccer:
			filter = Filter_Creator()
			if 'league_cond' not in gflags.FLAGS:
				f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
				league_conds = json.load(f_exp)
				for league_cond in league_conds:
					filter.execute(league_cond)
			else:
				filter.execute(json.loads(gflags.FLAGS.league_cond))
		else:
			filter = Ten_Filter_Creator()
			filter.execute()

	def test(self):
		if gflags.FLAGS.soccer:
			tester = Tester_Creator()
			if 'league_cond' not in gflags.FLAGS:
				f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
				league_conds = json.load(f_exp)
				for league_cond in league_conds:
					tester.execute(league_cond)
			else:
				tester.execute(json.loads(gflags.FLAGS.league_cond))
		else:
			tester = Ten_Tester_Creator()
			tester.execute()
	
	def analysis(self):
		if gflags.FLAGS.soccer:
			analysis = Analysis_Creator()
			if 'league_cond' not in gflags.FLAGS:
				f_exp = codecs.open('../db/league_conds.json', 'r', encoding='utf-8')
				league_conds = json.load(f_exp)
				for league_cond in league_conds:
					analysis.execute(league_cond)
			else:
				analysis.execute(json.loads(gflags.FLAGS.league_cond))
		else:
			analysis = Ten_Analysis_Creator()
			analysis.execute()
	
	def predict(self):
		predictor = Predictor()	
		predictor.predict()	

	def close(self):
		global conn
		conn.close()
