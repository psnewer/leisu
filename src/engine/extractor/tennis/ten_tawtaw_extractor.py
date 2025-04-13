#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import math

class TEN_TAWTAW_EXTRACTOR(object):
	def __init__(self):
		self.name = 'TEN_TAWTAW'
		self.params = {}

	def process(self,seasons,ext_dir):
		for season in seasons:
			season_str = "season='%s'"%season
			sql_str = "select tour,season,date,home,away,home_score,away_score,sets from tennis where %s order by date desc"%(season_str)
			df = pd.read_sql_query(sql_str,cont)
			df = self.generate_rows(df)
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

		# 应用解析和规则生成到整个 DataFrame
	def generate_rows(self, df):
		rows = []
		for idx, row in df.iterrows():
			home_team = row['home']
			away_team = row['away']
			procedure = row['sets']
        
        	# 分别生成 home_team 和 away_team 的行
			home_score,home_profit = self.analyze_match_scores(procedure, is_home=True)
			away_score,away_profit = self.analyze_match_scores(procedure, is_home=False)

			if (home_score is not None and away_score is not None): 
				seal = 1
				if (home_profit == -1 or away_profit == -1):
					seal = 0
				elif (home_profit == -0.5 or away_profit == -0.5):
					seal = 0.5
        
				# 添加 home_team 的数据
				rows.append({
					'team': home_team,
					'tour': row['tour'],
					'season': row['season'],
					'date': row['date'],
					'score': home_score,
					'profit': home_profit,
					'seal': seal,
					'home_team': home_team,
					'away_team': away_team,
					'home_score': row['home_score'],
					'away_score': row['away_score'],
					'procedure': row['sets']
				})
			
				# 添加 away_team 的数据
				rows.append({
					'team': away_team,
					'tour': row['tour'],
					'season': row['season'],
					'date': row['date'],
					'score': away_score,
					'profit': away_profit,
					'seal': seal,
					'home_team': home_team,
					'away_team': away_team,
					'home_score': row['home_score'],
					'away_score': row['away_score'],
					'procedure': row['sets']
				})

		return pd.DataFrame(rows, columns = ['team','tour','season','date','home_team','away_team','home_score','away_score','procedure','score','profit','seal'])
		
	def analyze_match_scores(self, score_list, is_home):
		results = []
		score_list = json.loads(score_list)
		for match in score_list:	
			team_break = False  # 是否先破发
			team_broken = False  # 是否先被破发
			team_break_back = False  # 破发方是否被追回
			team_broken_back = False  # 被破发方是否追回
			team_win = None  # 是否赢下比赛	
			for entry in match[:-1]:
				home, away, service = map(int, entry.split("-"))
				if service == 2:
					break
				if not team_break and not team_broken:
					# **检查破发情况**
					if service == 0:  # 主队发球
						if home < away:  
							if is_home:
								team_broken = True
							else:
								team_break = True
						elif home > away + 1: 
							if is_home:
								team_break = True
							else:
								team_broken = True
					else:  # 客队发球
						if away < home:  
							if is_home:
								team_break = True
							else:
								team_broken = True
						elif away > home + 1: 
							if is_home:
								team_broken = True
							else:
								team_break = True	
				if team_break or team_broken:
					if service == 0 and (home - away == 0 or home - away == 1):  # 主队发球
						if team_break:
							team_break_back = True
						else:
							team_broken_back = True
						break
					elif service == 1 and (away - home == 0 or away - home == 1):  # 客队发球           
						if team_break:
							team_break_back = True
						else:
							team_broken_back = True
						break
			if match:		
				home, away, service = map(int, match[-1].split("-"))
				team_win = (home > away) if is_home else (away > home)
				if team_win is not None:
					results.append({
                            "team_break": team_break,
                            "team_broken": team_broken,
                            "team_break_back": team_break_back,
                            "team_broken_back": team_broken_back,
                            "team_win": team_win
                        })

		score = 0
		profit = 0
		if len(results):
			if results[0]['team_break'] or (results[0]['team_win'] and not results[0]['team_broken']):
				score = -1
			if (results[0]['team_break'] and results[0]['team_break_back']):
				score = 1
			elif len(results) > 1:
				if (results[0]['team_break']):
					if (results[1]['team_broken']):
						score = 0.5
					elif (results[1]['team_break'] and results[1]['team_break_back']):
						score = -0.5
				elif (results[0]['team_win']) and (not results[0]['team_break']) and (not results[0]['team_broken']):
					if (results[1]['team_broken']):
						score = 1.5
					elif (not results[1]['team_win']):
						score = 2
			profit = score
			if score > 0:
				if (results[-1]['team_win']):
					profit = 0
			elif score < 0:
				if 	(not results[-1]['team_win']):
					profit = 0	

		return score,profit
	
	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
		