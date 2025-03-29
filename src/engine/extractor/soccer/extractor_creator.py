# -*- coding: utf-8 -*-
from extractor import *

class Extractor_Creator(object):
	def __init__(self):
		self.extractors = {}
		self.extractor_cand = []
		f_exp = codecs.open(gflags.FLAGS.extract_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			extractor_ins = eval(candstr)
			extractor_ins.setParams(cand['params'])
			self.extractor_cand.append(extractor_ins)

	def execute(self,condition):
		# condition = json.loads(condition)
		league_str = condition['league']
		league_cond = "league='%s'"%league_str
		league_dir = os.path.abspath(gflags.FLAGS.res_path + 'soccer/extract/' + league_str)
		mkdir(league_dir)
		if 'serryname' not in condition:
			cond = [league_cond]
			cond_str = ' and '.join(cond)
			sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
			seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
			if (gflags.FLAGS.predict):
				seasons = [max(seasons, key=str)]
			for extractor in self.extractor_cand:
				extractor_dir = league_dir + '/' + extractor.name
				mkdir(extractor_dir)
				extractor.process(cond_str,seasons,extractor_dir)
		else:
			for serryname in condition['serryname']:
				serry_dir = league_dir+'/'+serryname
				mkdir(serry_dir)
				serry_cond = "serryname='%s'"%serryname
				cond = [league_cond,serry_cond]
				cond_str = ' and '.join(cond)
				sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
				seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
				if (gflags.FLAGS.predict):
					seasons = [max(seasons, key=str)]
				for extractor in self.extractor_cand:
					extractor_dir = serry_dir + '/' + extractor.name
					mkdir(extractor_dir)
					extractor.process(cond_str,seasons,extractor_dir)
		
