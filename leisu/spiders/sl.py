# -*- coding: utf-8 -*-
import scrapy
import codecs
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import re
import logging
import logging.config
import datetime
from datetime import timedelta, datetime
from leisu.items import Match,Odds

class SlSpider(scrapy.Spider):
	name = 'sl'
	logging.config.fileConfig("./log/logging.conf")
	logspider = logging.getLogger("spider")

	def __init__(self,category=None,*args,**kwargs):
		self.allowed_domains = ['zq.win007.com']
		self.start_urls = ['http://zq.win007.com/jsData/infoHeader.js']
		self.dataPage = ''
        	self.cand_data = codecs.open("./src/db/leagues.conf",'r+',encoding='utf-8')
        	self.cand = json.load(self.cand_data)
		self.category = category

	def parse(self, response):
		self.dataPage = "http://zq.win007.com"
		self.logspider.info(self.dataPage)
		base_url = 'http://zq.win007.com/cn/{}/{}/{}.html'
		#base_url = 'http://zq.win007.com/jsData/matchResult/{}/s{}.js?version={}'
		#获取洲际
		continent_info = {'1':u'欧洲', '2':u'美洲', '3':u'亚洲', '4':u'大洋洲', '5':u'非洲'}
		str = response.text
		league_line = re.findall(r'= (\[.*?\]\])',str)
		for line in league_line:
			league_info = json.loads(line,encoding='utf-8')
			continent_id = league_info[3]
			continent = continent_info[continent_id]
			country = league_info[1]
			leagues = league_info[4]
			if country not in self.cand:
				continue
			for league_line in leagues:
				league_strs = league_line.split(',')
				league_id = league_strs[0]
				league = league_strs[1]
				match_type = league_strs[3]
				if league not in self.cand[country]:
					continue
#				if league != u'日职乙':
#					continue
				if match_type == '0':
					match_type = 'League'
				else:
					match_type = 'SubLeague'
				for idx,season in enumerate(league_strs[4:]):
					if self.category is not None and idx != 0:
						continue
					url = base_url.format(match_type,season,league_id)
#					url = 'http://zq.win007.com/cn/SubLeague/2009/284.html'
#					league_id = '284'
#					season = '2009'
					yield scrapy.Request(url, callback=self.parseSeason, meta={'continent':continent, 'country':country, 'league':league, 'season':season, 'league_id':league_id, 'idx':idx})
												 
	def parseSeason(self, response):
		continent = response.meta['continent']
		country = response.meta['country']
		league = response.meta['league']
		season = response.meta['season']
		league_id = response.meta['league_id']
		idx = response.meta['idx']
		href = response.xpath(u"//script[contains(@src,'matchResult')]/@src").re_first('(.+)')
		url = self.dataPage + href
		yield scrapy.Request(url, callback=self.parseSubLeague, meta={'continent':continent, 'country':country, 'league':league, 'season':season, 'league_id':league_id, 'idx':idx}, dont_filter = True)

	def parseSubLeague(self, response):
		continent = response.meta['continent']
		country = response.meta['country']
		league = response.meta['league']
		season = response.meta['season']
		league_id = response.meta['league_id']
		idx = response.meta['idx']
		today=time.strftime("%Y%m%d%H%M", time.localtime())
		tomorrow = (datetime.today() + timedelta(7)).strftime('%Y%m%d%H%M')
		yesterday = (datetime.today() + timedelta(-7)).strftime('%Y%m%d%H%M')
		if 'arrSubLeague' in response.text:
			arr_subleague = re.search(r'arrSubLeague = (\[\[.*?\]\])',response.text).group(1)
			subleagues = eval(arr_subleague)
			base_url = 'http://zq.win007.com/jsData/matchResult/{}/s{}.js?version={}'
			for sub in subleagues:
				if u'升级' not in sub[1] and u'降级' not in sub[1] and u'附加' not in sub[1] and u'冠军' not in sub[1] and u'决赛' not in sub[1]:
					serryname = sub[1]
					sub_id = sub[0]
					serryid = league_id + '_' + str(sub_id)
					req_time = time.strftime('%Y%m%d%H', time.localtime())
					url = base_url.format(season,serryid,req_time)
					yield scrapy.Request(url, callback=self.parseRound, meta={'continent':continent, 'country':country, 'league':league, 'season':season, 'serryname':serryname, 'serryid':serryid + '_'+ str(season), 'idx':idx}, dont_filter = True)
		else:
			list_all_team = self.team_data_id(response)
			rounds = re.findall(r'jh\[(.*?)\] = (\[\[.*?\]\])',response.text)
			for round_str in rounds:
				if len(round_str) < 2:
					continue
				stage = round_str[0].strip('"').split('_')[1]
				matches = re.findall(r'\[(.*?)\]',round_str[1].replace('[[','[').replace(']]',']'))
				for match_str in matches:
					match_info = match_str.split(',')
					if len(match_info) < 11:
						continue
					date = ''.join(re.findall(r'(\d+)',match_info[3]))
					if date > tomorrow:
						continue
					if (int(match_info[4]) not in list_all_team or int(match_info[5]) not in list_all_team) or len(date)==0:
						continue
					home_team = list_all_team[int(match_info[4])]
					away_team = list_all_team[int(match_info[5])]
					scores = match_info[6].strip("'").split('-')
					if len(scores) < 2 and idx == 0:
						home_goal = -1
						away_goal = -1
					elif len(scores) < 2 and idx > 0:
						self.logspider.warn(continent+" "+country+" "+league+" "+season+" "+stage)
						continue
					else:
						home_goal = scores[0]
						away_goal = scores[1]
					if home_goal < 0 and date < today:
						continue
					if self.category is not None:
						if date < yesterday:
							continue
					home_odds = '0.9'
					away_odds = '0.9'
					pan = match_info[10]
					if len(pan) == 0:
						continue
					match = Match()
					match['continent'] = continent
					match['country'] = country
					match['league'] = league
					match['season'] = season
					match['stage'] = stage
					match['date'] = date
					match['serryid'] = league_id + '_' + str(season)
					match['serryname'] = u'联赛'
					match['home_team'] = home_team
					match['away_team'] = away_team
					match['home_goal'] = home_goal
					match['away_goal'] = away_goal
					match['home_odds'] = home_odds
					match['away_odds'] = away_odds
					match['pan'] = pan
					yield match

	
	def team_data_id(self,response):
		# 获取每个队伍的id和队名
		team_str = re.search(r'arrTeam = (\[\[.*?\]\])',response.text).group(1)
		teams = eval(team_str)
		list_all_team = {}
		for item in teams:
			list_all_team[item[0]]=item[1]
		return list_all_team		

	def parseRound(self, response):
		continent = response.meta['continent']
		country = response.meta['country']
		league = response.meta['league']
		season = response.meta['season']
		idx = response.meta['idx']
		serryname = response.meta['serryname']
		serryid = response.meta['serryid']
		list_all_team = self.team_data_id(response)
		rounds = re.findall(r'jh\[(.*?)\] = (\[\[.*?\]\])',response.text)
		today=time.strftime("%Y%m%d%H%M", time.localtime())
		tomorrow = (datetime.today() + timedelta(7)).strftime('%Y%m%d%H%M')
		yesterday = (datetime.today() + timedelta(-7)).strftime('%Y%m%d%H%M')
		for round_str in rounds:
			if len(round_str) < 2:
				continue
			stage = round_str[0].strip('"').split('_')[1]
			matches = re.findall(r'\[(.*?)\]',round_str[1].replace('[[','[').replace(']]',']'))
			for match_str in matches:
				match_info = match_str.split(',')
				if len(match_info) < 11:
					continue
				date = ''.join(re.findall(r'(\d+)',match_info[3]))
				if date > tomorrow:
					continue
				if (int(match_info[4]) not in list_all_team or int(match_info[5]) not in list_all_team) or len(date)==0:
					continue
				home_team = list_all_team[int(match_info[4])]
				away_team = list_all_team[int(match_info[5])]
				scores = match_info[6].strip("'").split('-')
				if len(scores) < 2 and idx == 0:
					home_goal = -1
					away_goal = -1
				elif len(scores) < 2 and idx > 0:
					self.logspider.warn(continent+" "+country+" "+league+" "+season+" "+stage)
					continue
				else:
					home_goal = scores[0]
					away_goal = scores[1]
				if home_goal < 0 and date < today:
					continue
				if self.category is not None:
					if date < yesterday:
						continue
				home_odds = '0.9'
				away_odds = '0.9'
				pan = match_info[10]
				if len(pan) == 0:
					continue
				match = Match()
				match['continent'] = continent
				match['country'] = country
				match['league'] = league
				match['season'] = season
				match['stage'] = stage
				match['date'] = date
				match['serryid'] = serryid
				match['serryname'] = serryname
				match['home_team'] = home_team
				match['away_team'] = away_team
				match['home_goal'] = home_goal
				match['away_goal'] = away_goal
				match['home_odds'] = home_odds
				match['away_odds'] = away_odds
				match['pan'] = pan
				yield match


