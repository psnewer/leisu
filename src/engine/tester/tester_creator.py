# -*- coding: utf-8 -*-
from tester import *

class Tester_Creator(object):
	def __init__(self):
		self.tester_cand = []
		f_exp = codecs.open(gflags.FLAGS.test_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			tester_ins = eval(candstr)
			tester_ins.setParams(cand['params'])
			self.tester_cand.append(tester_ins)

	def execute(self,condition):
		# condition = json.loads(condition)
		league_str = condition['league']
		league_cond = "league='%s'"%league_str
		league_dir = os.path.abspath(gflags.FLAGS.test_path + league_str)
		mkdir(league_dir)
		if 'serryname' not in condition:
			cond = [league_cond]
			cond_str = ' and '.join(cond)
			sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
			seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
			for tester in self.tester_cand:
				test_dir = league_dir + '/' + tester.name
				mkdir(test_dir)
				tester.process(cond_str,seasons,test_dir)
		else:
			for serryname in condition['serryname']:
				serry_dir = league_dir+'/'+serryname
				mkdir(serry_dir)
				serry_cond = "serryname='%s'"%serryname
				cond = [league_cond,serry_cond]
				cond_str = ' and '.join(cond)
				sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
				seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
				for tester in self.tester_cand:
					tester_dir = serry_dir + '/' + tester.name
					mkdir(tester_dir)
					tester.process(cond_str,seasons,tester_dir)
						
