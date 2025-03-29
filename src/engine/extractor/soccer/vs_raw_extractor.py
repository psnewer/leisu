#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_extractor import ABSTRACT_EXTRACTOR

class VS_RAW_EXTRACTOR(ABSTRACT_EXTRACTOR):
	def __init__(self):
		self.name = 'VS_RAW'
		self.params = {}

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			season_str = "season='%s'"%season
			_cond_str = cond_str + ' and ' + season_str
			sql_str = "select league,season,serryid,serryname,stage,date,home_team,away_team,score,procedure from matches where %s order by date desc"%(_cond_str)
			df = pd.read_sql_query(sql_str,conn)
			df = self.generate_rows(df)
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def analyze_procedure(self, procedure, team, is_home):
    	# 将 JSON 字符串解析为 Python 对象
		procedure = json.loads(procedure)
    
    	# 初始化标志位
		raw_win = None
		taw_win = None
		raw_draw = None
		taw_draw = None
		raw = 0
		taw = 0
		has_goal = False
		team_always_behind = True
		team_always_ahead = True
		draw_at_some_point = False
		no_goals = True
		team_won = False

		if (not procedure):
			return [None,None,None,None,None,None]
    
    	# 遍历 procedure 列表中的每个记录，查找进球和平局信息
		for event in procedure:
			score_home = int(event['score_home'])
			score_away = int(event['score_away'])
        
        	# 根据是否是主场，确定 team 是 home 还是 away
			if is_home:
				team_score = score_home
				opponent_score = score_away
				if (event['time'] == '0'):
					raw_win = event['home']
					taw_win = event['away']
			else:
				team_score = score_away
				opponent_score = score_home
				if (event['time'] == '0'):
					raw_win = event['away']
					taw_win = event['home']
        
        	# 判断是否有进球
			if score_home > 0 or score_away > 0:
				has_goal = True
				no_goals = False  # 有进球时，说明比赛中没有零进球

            	# 检查是否在某时打平
				if score_home == score_away:
					draw_at_some_point = True
					team_always_behind = False
					team_always_ahead = False
					if raw_draw is None:
						raw_draw,taw_draw = (event['home'],event['away']) if is_home else (event['away'],event['home'])
            
            	# 判断 team 是否一直落后
				if team_score > opponent_score:
					team_always_behind = False
				elif team_score < opponent_score:
					team_always_ahead = False
            
            	# 判断最后 team 的胜负情况
				if event == procedure[-1]:  # 检查最后一个事件
					if team_score > opponent_score:
						team_won = True

    	# 应用规则：raw 列根据不同情况设定
		if no_goals:
			raw = 1
		elif has_goal and team_always_behind:
			raw = -1
		elif has_goal and draw_at_some_point and not team_won:
			raw = 0
		else:
			raw = 1

		if no_goals:
			taw = 1
		elif has_goal and team_always_ahead:
			taw = -1
		elif has_goal and draw_at_some_point and team_won:
			taw = 0
		else:
			taw = 1

		return raw,taw,raw_win,taw_win,raw_draw,taw_draw

		# 应用解析和规则生成到整个 DataFrame
	def generate_rows(self, df):
		rows = []
		for idx, row in df.iterrows():
			home_team = row['home_team']
			away_team = row['away_team']
			procedure = row['procedure']
        
        	# 分别生成 home_team 和 away_team 的行
			home_raw,home_taw,home_raw_win,home_taw_win,home_raw_draw,home_taw_draw = self.analyze_procedure(procedure, home_team, is_home=True)
			away_raw,away_taw,away_raw_win,away_taw_win,away_raw_draw,away_taw_draw = self.analyze_procedure(procedure, away_team, is_home=False)

			if (home_raw is not None and away_raw is not None):
        
				# 添加 home_team 的数据
				rows.append({
					'team': home_team,
					'raw': home_raw,
					'taw': home_taw,
					'raw_win': home_raw_win,
					'taw_win': home_taw_win,
					'raw_draw': home_raw_draw,
					'taw_draw': home_taw_draw,
					'league': row['league'],
					'season': row['season'],
					'serryid': row['serryid'],
					'serryname': row['serryname'],
					'stage': row['stage'],
					'date': row['date'],
					'home_team': home_team,
					'away_team': away_team,
					'score': row['score'],
					'procedure': row['procedure']
				})
			
				# 添加 away_team 的数据
				rows.append({
					'team': away_team,
					'raw': away_raw,
					'taw': away_taw,
					'raw_win': away_raw_win,
					'taw_win': away_taw_win,
					'raw_draw': away_raw_draw,
					'taw_draw': away_taw_draw,
					'league': row['league'],
					'season': row['season'],
					'serryid': row['serryid'],
					'serryname': row['serryname'],
					'stage': row['stage'],
					'date': row['date'],
					'home_team': home_team,
					'away_team': away_team,
					'score': row['score'],
					'procedure': row['procedure']
				})

		return pd.DataFrame(rows, columns = ['team','raw','taw','raw_win','taw_win','raw_draw','taw_draw','league','season','serryid','serryname','stage','date','home_team','away_team','score','procedure'])

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)

		