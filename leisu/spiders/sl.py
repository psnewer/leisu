# -*- coding: utf-8 -*-
import scrapy
import json
import sys
import time
import logging
import logging.config
import datetime
from datetime import timedelta, datetime
from leisu.items import Match,Odds

class SlSpider(scrapy.Spider):
	name = 'sl'
	allowed_domains = ['leisu.com']
	start_urls = ['http://leisu.com/']
	dataPage = ''
	logging.config.fileConfig("/Users/miller/Documents/workspace/leisu/log/logging.conf")
	logspider = logging.getLogger("spider")

	def parse(self, response):
		href = response.xpath(u"//li/a[text()[contains(.,'资料库')]]/@href").re_first("//(.+)")
		self.dataPage = "https://" + href
		self.logspider.info(self.dataPage)
		yield scrapy.Request(self.dataPage, callback=self.parseData)

	def parseData(self, response):
		#获取洲际
		sZ = response.xpath(u"//div[@class='lallrace_nav_wrap']/ul/li/a/text()").extract()
		sA = response.xpath(u"//div[@class='match-content']")
		parts1 = sA.xpath(u".//div[contains(@class,'match-class')]")
		for idx,part1 in enumerate(parts1):
			sB = part1.xpath(u".//div[@class='country-match']/div/div/ul/li")
			continent = sZ[idx]
			for sC in sB:
				country = sC.xpath(u".//a/text()").re_first('(.+)')
				sD = sC.xpath(u".//div/a")
				for _league in sD:
					league = _league.xpath(".//text()").re_first('(.+)')
					href = _league.xpath(".//@href").re_first('(.+)')
					url = self.dataPage + href
					yield scrapy.Request(url, callback=self.parseLeague, meta={'continent':continent, 'country':country, 'league':league})

	def parseLeague(self, response):
		continent = response.meta['continent']
		country = response.meta['country']
		league = response.meta['league']
		sA = response.xpath(u"//div/ul/li/a[text()[contains(.,'赛季')]]")
		for sB in sA:
			season = sB.xpath(u".//text()").re_first('(.+)')
			href = sB.xpath(u".//@href").re_first('/(.+)')
			url = self.dataPage + href
			yield scrapy.Request(url, callback=self.parseSeason, meta={'season':season, 'continent':continent, 'country':country, 'league':league})
	
	def parseSeason(self, response):
		continent = response.meta['continent']
		country = response.meta['country']
		league = response.meta['league']
		season = response.meta['season']
		today=time.strftime("%Y%m%d", time.localtime())
		tomorrow = (datetime.today() + timedelta(7)).strftime('%Y%m%d')
		sA = response.xpath(u"//div[contains(@id,'stage-content')]")
		for _sB in sA:
			serryid = _sB.xpath(u".//@data-id").re_first('(.+)')
			serryname = response.xpath(u"//ol/li[contains(@data-id,'%s')]/@data-name"%serryid).re_first('(.+)')
			sB = _sB.xpath(u".//table[contains(@class,'schedule-table')]")
			sC = sB.xpath('.//tr')[1:]
			date_reserve = ''
			for sD in sC:
				stage = sD.xpath(u".//@data-round").re_first('(.+)')
				date = sD.xpath(u".//@data-date").re_first('(.+)')
				_time = sD.xpath(u".//td[contains(@class,'td-date')]/text()").re_first('(.+)')
				if _time is None:
					_time = sD.xpath(u".//td[contains(@class,'bd-r')]/text()").re_first('(.+)')
				if _time is None:
					_time = sD.xpath(u".//td[contains(@class,'text-a-l')]/text()").re_first('(.+)')
					if _time is None:
						continue
					else:
						_time = _time.split(' ')[0]
					date_reserve = ''.join(_time.split("/"))
					continue
				ts = _time.strip().split("  ")
				ts0 = ts[0].split(":")
				if (len(date) < 8 and len(ts) == 2):
					date = ''.join(ts[1].split("/"))
				if (len(date) < 8):
					date = date_reserve
					if (len(date) < 8):
						self.logspider.warn(continent+" "+country+" "+league+" "+season+" "+stage)
						continue
				if (date > tomorrow):
					continue
				_time = date + ts0[0] + ts0[1]
				teams = sD.xpath(u".//td/a/font/span/text()").extract()
				scores = sD.xpath(u".//td/span/text()").extract()
				if (len(teams) < 2 or len(scores) < 2):
					if (date < today or len(teams) < 2):
						clause = continent+" "+country+" "+league+" "+season+" "+serryid+" "+stage+" "+' '.join(teams)+" "+' '.join(scores)
						self.logspider.warn(clause)
						continue
					else:
						scores = [-1,-1]
				home_team = teams[0]
				away_team = teams[1]
				home_goal = scores[0]
				away_goal = scores[1]
				match = Match()
				match['continent'] = continent
				match['country'] = country
				match['league'] = league
				match['season'] = season
				match['stage'] = stage
				match['date'] = _time
				match['serryid'] = serryid
				match['serryname'] = serryname
				match['home_team'] = home_team
				match['away_team'] = away_team
				match['home_goal'] = home_goal
				match['away_goal'] = away_goal
				yield match
