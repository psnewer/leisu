# -*- coding: utf-8 -*-
import scrapy
import codecs
import json
import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')
import time
import re
# import logging
# import logging.config
import datetime
from datetime import timedelta, datetime
from leisu.items import Match

class GjSpider(scrapy.Spider):
	name = 'gj'
	# logging.config.fileConfig("./log/logging.conf")
	# logspider = logging.getLogger("spider")

	def __init__(self,category=None,*args,**kwargs):
		self.allowed_domains = ['zq.titan007.com']
		self.start_urls = ['http://zq.titan007.com/jsData/infoHeader.js']
		self.dataPage = ''
		self.cand_data = codecs.open("./src/db/games_cand.json",'r+',encoding='utf-8')
		self.cand = json.load(self.cand_data)
		self.category = category

	def parse(self, response):
		self.dataPage = "http://zq.titan007.com"
		# self.logspider.info(self.dataPage)
		base_url = 'http://zq.titan007.com/cn/{}/{}/{}.html'
		#base_url = 'http://zq.titan007.com/jsData/matchResult/{}/s{}.js?version={}'
		#获取洲际
		continent_info = {'0':u'国际','1':u'欧洲', '2':u'美洲', '3':u'亚洲', '4':u'大洋洲', '5':u'非洲'}
		str = response.text
		league_line = re.findall(r'=\s*(\[.*?\]\])',str)
		for line in league_line:
			league_info = json.loads(line)
			continent_id = league_info[3]
			continent = continent_info[continent_id]
			country = league_info[1]
			leagues = league_info[4]
			# if country not in self.cand:
			# 	continue
			for league_line in leagues:
				league_strs = league_line.split(',')
				league_id = league_strs[0]
				league = league_strs[1]
				match_type = league_strs[3]
				if league not in self.cand:
					continue
				# if league != u'法甲':
				# 	continue
				match_type = 'CupMatch'
				for idx,season in enumerate(league_strs[4:]):
					if self.category == 'predict':
						if idx != 0:
							continue
					url = base_url.format(match_type,season,league_id)
					# if season != "2022-2023":
					# 	continue
					yield scrapy.Request(url, callback=self.parseSeason, meta={'continent':continent, 'country':country, 'league':league, 'season':season, 'league_id':league_id, 'idx':idx})
												 
	def parseSeason(self, response):
		href = response.xpath(u"//script[contains(@src,'matchResult')]/@src").re_first('(.+)')
		url = self.dataPage + href
		yield scrapy.Request(url, callback=self.parseSubLeague, meta=response.meta, dont_filter = True)

	def parseSubLeague(self, response):
		base_url = 'http://zq.titan007.com/jsData/matchResult/{}/c{}.js?version={}'
		req_time = time.strftime('%Y%m%d%H', time.localtime())
		if 'arrCup' in response.text:
			arr_league = re.search(r'arrCup\s*=\s*(\[.*?\])',response.text).group(1)
			leagues = eval(arr_league)
			response.meta['league'] = leagues[3]
		if 'arrCupKind' in response.text:
			arr_subleague = re.search(r'arrCupKind\s*=\s*(\[\[.*?\]\])',response.text).group(1)
			subleagues = eval(arr_subleague)
			for sub in subleagues:
				sub_id = sub[0]
				serryid = response.meta['league_id'] + '_' + str(sub_id)
				url = base_url.format(response.meta['season'],response.meta['league_id'],req_time)
				response.meta['serryname'] = sub[4]
				response.meta['serryid'] = serryid
				yield scrapy.Request(url, callback=self.parseRound, meta=response.meta, dont_filter = True)
		else:
			response.meta['serryname'] = u'League'
			response.meta['serryid'] = response.meta['league_id']
			url = base_url.format(response.meta['season'],response.meta['league_id'],req_time)
			yield scrapy.Request(url, callback=self.parseRound, meta=response.meta, dont_filter = True)		

	def parseRound(self, response):
		list_all_team = self.team_data_id(response)
		rounds = re.findall(r'jh\[(.*?)\]\s*=\s*(\[\[.*?\]\])',response.text)
		today=time.strftime("%Y%m%d%H%M", time.localtime())
		tomorrow = (datetime.today() + timedelta(3)).strftime('%Y%m%d%H%M')
		yesterday = (datetime.today() + timedelta(-3)).strftime('%Y%m%d%H%M')
		for round_str in rounds:
			if len(round_str) < 2:
				continue
			stage = round_str[0].strip('"')
			matches = re.findall(r'\[([^\[\]]*?)\]',round_str[1])
			for match_str in matches:
				match_info = match_str.split(',')
				if len(match_info) < 11:
					continue
				date = ''.join(re.findall(r'(\d+)',match_info[3]))
				if self.category == 'predict' and (date < yesterday or date > tomorrow):
					continue
				elif date >= tomorrow:
					continue
				if (int(match_info[4]) not in list_all_team or int(match_info[5]) not in list_all_team) or len(date)==0:
					continue
				home_team = list_all_team[int(match_info[4])]
				away_team = list_all_team[int(match_info[5])]
				# scores = match_info[6].strip("'").split('-')
				# if len(scores) < 2:
				# 	self.logspider.warn(continent+" "+country+" "+league+" "+season+" "+stage)
				# 	continue
				# else:
				# 	home_goal = int(scores[0])
				# 	away_goal = int(scores[1])
				# if home_goal < 0 and date < today:
				# 	continue
				score = match_info[6]
				half_score = match_info[7]
				if len(score) < 3 or len(half_score) < 3:
					if date < today:
						continue
				# if self.category is not None:
				# 	if date < yesterday:
				# 		continue
				# home_odds = '0.9'
				# away_odds = '0.9'
				# pan = match_info[10]
				# if len(pan) == 0:
				# 	continue
				match = Match()
				match['match_id'] = match_info[0]
				match['continent'] = response.meta['continent']
				match['country'] = response.meta['country']
				match['league'] = response.meta['league']
				match['season'] = response.meta['season']
				match['stage'] = stage
				match['date'] = date
				match['serryid'] = response.meta['serryid']
				match['serryname'] = response.meta['serryname']
				match['home_team'] = home_team
				match['away_team'] = away_team
				match['score'] = score
				match['half_score'] = half_score
				s1 = match_info[0][:2]
				s2 = match_info[0][2:4]
				url = 'https://livestatic.titan007.com/jsData/{}/{}/{}.js'.format(s1,s2,match_info[0])
				yield scrapy.Request(url, callback=self.parseProcedure, meta={'match': match}, dont_filter = True)

	def parseProcedure(self, response):
		if 'sOdds' not in response.text:
			response.meta['match']['procedure'] = '[]'
		else:
			pattern = r'sOdds=\[(.*?)\];'
			sOdds = re.search(pattern, response.text).group(1)
			events = re.findall(r'\[([^[\]]+)\]', sOdds)
			result = []
			for event in events:
				strs = event.split(',')  # 根据逗号拆分字符串
				data = {
					'time': strs[0].strip(), 
					'score_home': strs[1].strip(),
					'score_away': strs[2].strip(),
					'home': strs[36].strip(),
					'draw': strs[37].strip(),
					'away': strs[38].strip(),
					}
				data['time'] = '-1' if data['time'] == "'早餐'" else data['time']
				data['time'] = '0' if data['time'] == "'未开场'" else data['time']
				result.append(data)
			response.meta['match']['procedure'] = json.dumps(result)
		yield response.meta['match']
				

	def team_data_id(self,response):
		# 获取每个队伍的id和队名
		team_str = re.search(r'arrTeam\s*=\s*(\[\[.*?\]\])',response.text).group(1)
		teams = eval(team_str)
		list_all_team = {}
		for item in teams:
			list_all_team[item[0]]=item[3]
		return list_all_team


