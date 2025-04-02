#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_extractor import ABSTRACT_EXTRACTOR
import math

class VS_RAWRAW_EXTRACTOR(ABSTRACT_EXTRACTOR):
	def __init__(self):
		self.name = 'VS_RAWRAW'
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

	def analyze_procedure(self, procedure, score, is_home):
    	# 将 JSON 字符串解析为 Python 对象
		procedure = json.loads(procedure)

		scores = score.strip("'").split('-')
		home_score = int(scores[0]) if len(scores) > 1 else None
		away_score = int(scores[1]) if len(scores) > 1 else None
    	# 初始化标志位
		raw_win = None
		taw_win = None
		raw_draw = None
		taw_draw = None
		raw = 0
		taw = 0
		rawraw = 0
		bought = False
		team_always_behind = True
		team_always_ahead = True
		draw_at_some_point = False
		no_goals = True
		team_draw = False
		team_won = False
		team_lose = False
		pre_score_home = 0
		pre_score_away = 0
		draw_time = 0
		home = None
		draw = None
		away = None
		draw_0 = None
		man = -1
		filter = None

		if (not procedure):
			return [None,None,None,None,None,None,None]
    
    	# 遍历 procedure 列表中的每个记录，查找进球和平局信息
		for event in procedure:
			score_home = int(event['score_home'])
			score_away = int(event['score_away'])
			time = int (event('time'))

			if score_home > pre_score_home and score_away > pre_score_away:
				return [0,0,0,0,0,0,0]
			pre_score_home = score_home
			pre_score_away = score_away
        
        	# 根据是否是主场，确定 team 是 home 还是 away
			if is_home:
				team_score = score_home
				opponent_score = score_away
			else:
				team_score = score_away
				opponent_score = score_home
        
        	# 判断是否有进球
			if score_home > 0 or score_away > 0:
				has_goal = True
				no_goals = False  # 有进球时，说明比赛中没有零进球

				if raw_win is None:
					raw_win,taw_win = (1.0 + 1.0/(float(event['away']) - 1.0) if event['away'] else math.nan,1.0 + 1.0/(float(event['home']) - 1.0) if event['home'] else math.nan) if is_home else (1.0 + 1.0/(float(event['home']) - 1.0) if event['home'] else math.nan,1.0 + 1.0/(float(event['away']) - 1.0) if event['away'] else math.nan)

				if score_away == 0 and not is_home:
					bought = True
					draw_0 = event('draw')
				elif score_home == 0 and is_home:
					bought = True
					draw_0 = event('draw')
            	# 检查是否在某时打平
				if score_home == score_away:
					if not draw_at_some_point:
						draw_time = time
						home = float(event('home'))
						draw = float(event('draw'))
						away = float(event('away'))
					draw_at_some_point = True
					team_always_behind = False
					team_always_ahead = False
					if raw_draw is None:
						raw_draw,taw_draw = (1.0 + 1.0/(float(event['away']) - 1.0) if event['away'] else math.nan,1.0 + 1.0/(float(event['home']) - 1.0) if event['home'] else math.nan) if is_home else (1.0 + 1.0/(float(event['home']) - 1.0) if event['home'] else math.nan,1.0 + 1.0/(float(event['away']) - 1.0) if event['away'] else math.nan)
            
            	# 判断 team 是否一直落后
				if team_score > opponent_score:
					team_always_behind = False
				elif team_score < opponent_score:
					team_always_ahead = False
            
            	# 判断最后 team 的胜负情况
				if event == procedure[-1]:  # 检查最后一个事件
					if home_score != score_home or away_score != score_away:
						if is_home:
							team_score = home_score
							opponent_score = away_score
						else:
							team_score = away_score
							opponent_score = home_score
					if team_score > opponent_score:
						team_won = True
					elif team_score < opponent_score:
						team_lose = True
					elif team_score == opponent_score:
						draw_at_some_point = True
						if not draw_at_some_point:
							draw_time = time
							home = float(event('home'))
							draw = float(event('draw'))
							away = float(event('away'))

    	# 应用规则：raw 列根据不同情况设定
		if no_goals or draw_at_some_point:
			raw = 0
		elif team_lose:
			raw = -1
			rawraw = -1
		elif team_won:
			raw = 1

		if no_goals or draw_at_some_point:
			taw = 0
		elif team_lose:
			taw = 1
		elif team_won:
			taw = -1

		if bought and not team_lose:
			rawraw = 1

		if no_goals or not bought:
			man = 0
		elif draw_at_some_point:
			if draw_time <= 45 and 0.5*(draw_0-1)*(home-1)-1 > (draw_0-draw) and 0.5*(draw_0-1)*(away-1)-1 > (draw_0-draw):
				filter = 'o3'
				if team_won or team_lose:
					man = 1
				else:
					man = 0
			else:
				filter = 'draw'
				if not team_won and not team_lose:
					man = 1
				else:
					man = 0

		return raw,taw,draw_0,home,draw,away,filter,man,rawraw

		# 应用解析和规则生成到整个 DataFrame
	def generate_rows(self, df):
		rows = []
		for idx, row in df.iterrows():
			home_team = row['home_team']
			away_team = row['away_team']
			procedure = row['procedure']
			score = row['score']
        	# 分别生成 home_team 和 away_team 的行
			home_raw,home_taw,home_draw_0,home_home,home_draw,home_away,home_filter,home_man,home_rawraw = self.analyze_procedure(procedure, score, is_home=True)
			away_raw,away_taw,away_draw_0,away_home,away_draw,away_away,away_filter,away_man,away_rawraw = self.analyze_procedure(procedure, score, is_home=False)

			if (home_raw is not None and away_raw is not None):
        
				# 添加 home_team 的数据
				rows.append({
					'team': home_team,
					'raw': home_raw,
					'taw': home_taw,
					'rawraw': home_rawraw,
					'draw_0': home_draw_0,
					'home': home_home,
					'draw': home_draw,
					'away': home_away,
					'filter': home_filter,
					'man': home_man,
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
					'rawraw': away_rawraw,
					'draw_0': away_draw_0,
					'home': away_home,
					'draw': away_draw,
					'away': away_away,
					'filter': away_filter,
					'man': away_man,
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

		return pd.DataFrame(rows, columns = ['team','raw','taw','rawraw','draw_0','home','draw','away','filter','man','league','season','serryid','serryname','stage','date','home_team','away_team','score','procedure'])

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)

		