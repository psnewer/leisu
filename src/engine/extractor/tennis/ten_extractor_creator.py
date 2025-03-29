# -*- coding: utf-8 -*-
from extractor import *

class Ten_Extractor_Creator(object):
	def __init__(self):
		self.extractors = {}
		self.extractor_cand = []
		f_exp = codecs.open(gflags.FLAGS.extract_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			extractor_ins = eval(candstr)
			self.extractor_cand.append(extractor_ins)

	def execute(self):
		# # condition = json.loads(condition)
		# league_str = condition['league']
		# league_cond = "league='%s'"%league_str
		# league_dir = os.path.abspath(gflags.FLAGS.extract_path + league_str)
		# mkdir(league_dir)
		# cond = [league_cond]
		# cond_str = ' and '.join(cond)
		sql_str = "select distinct season from tennis"
		seasons = pd.read_sql_query(sql_str,cont)['season'].to_numpy()
		if (gflags.FLAGS.predict):
			seasons = [max(seasons, key=str)]
		for extractor in self.extractor_cand:
			extractor_dir = os.path.abspath(gflags.FLAGS.extract_path + 'tennis') + '/' + extractor.name
			mkdir(extractor_dir)
			extractor.process(seasons,extractor_dir)

		
