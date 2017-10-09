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

class OddsSpider(scrapy.Spider):
	name = 'odds'
	allowed_domains = ['leisu.com']
	start_urls = ['http://leisu.com/']
	dataPage = ''
#	logging.config.fileConfig("/Users/miller/Documents/workspace/leisu/log/logging.conf")
#	logspider = logging.getLogger("spider")

	def parse(self, response):
		href = response.xpath(u"//li/a[text()[contains(.,'资料库')]]/@href").re_first("//(.+)")
		self.dataPage = "https://" + href
#		self.logspider.info(self.dataPage)
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
		tomorrow = (datetime.today() + timedelta(1)).strftime('%Y%m%d')
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
#						self.logspider.warn(continent+" "+country+" "+league+" "+season+" "+stage)
						continue
				if (date > tomorrow):
					continue
				_time = date + ts0[0] + ts0[1]
				teams = sD.xpath(u".//td/a/font/span/text()").extract()
				scores = sD.xpath(u".//td/span/text()").extract()
				if (len(teams) < 2 or len(scores) < 2):
					if (date < today or len(teams) < 2):
						clause = continent+" "+country+" "+league+" "+season+" "+serryid+" "+stage+" "+' '.join(teams)+" "+' '.join(scores)
#						self.logspider.warn(clause)
						continue
					else:
						scores = [-1,-1]
				home_team = teams[0]
				away_team = teams[1]
				href = sD.xpath(u".//td/a[text()='析']/@href").re_first('(.+)')
				url = 'https:' + href
				if date > '20170317' and date <= today:
					yield scrapy.Request(url, callback=self.parseStatistic, meta={'season':season, 'league':league, 'date':date, 'home_team':home_team, 'away_team':away_team})
			
	def parseStatistic(self, response):
		href = response.xpath(u"//div/ul/li/a[text()[contains(.,'三合一')]]/@href").re_first('(.+)')
		url = 'https:' + href
		yield scrapy.Request(url, callback=self.parseOdds, meta=response.meta)

	def parseOdds(self, response):
		league = response.meta['league']
		season = response.meta['season']
		date = response.meta['date']
		home_team = response.meta['home_team']
		away_team = response.meta['away_team']
		odds = Odds()
		odds['league'] = league
		odds['season'] = season
		odds['date'] = date
		odds['home_team'] = home_team
		odds['away_team'] = away_team
		sA = response.xpath(u".//table/tbody/tr[@data-company]")
		clear1 = False
		clear2 = False
		clear3 = False
		for sB in sA:
			o1 = sB.xpath(u"./td[contains(@class,'rangQiu')]")
			o2 = sB.xpath(u"./td[contains(@class,'biaoZhunPan')]")
			o3 = sB.xpath(u"./td[contains(@class,'daXiaoQiu')]")
			o1_first = o1.xpath(u".//tr[@class='first']")
			o2_first = o2.xpath(u".//tr[@class='first']")
			o3_first = o3.xpath(u".//tr[@class='first']")
			o1_first_tds = o1_first.xpath(u".//td[contains(@class,'tr-data')]")
			if (len(o1_first_tds) == 3 and not clear1):
				rangzhu = o1_first_tds[0].xpath(u".//span/text()").re_first('(.+)')
				rangpan = o1_first_tds[1].xpath(u".//text()").re_first('(.+)')
				rangke = o1_first_tds[2].xpath(u".//span/text()").re_first('(.+)')
				if rangzhu is not None and rangpan is not None and rangke is not None:
					odds['rangzhu'] = rangzhu
					odds['rangpan'] = rangpan
					odds['rangke'] = rangke
					clear1 = True
			o2_first_tds = o2_first.xpath(u".//td[contains(@class,'tr-data')]")
			if (len(o2_first_tds) ==3 and not clear2):
				zhusheng = o2_first_tds[0].xpath(u".//span/text()").re_first('(.+)')
				ping = o2_first_tds[1].xpath(u".//text()").re_first('(.+)')
				kesheng = o2_first_tds[2].xpath(u".//span/text()").re_first('(.+)')
				if zhusheng is not None and ping is not None and kesheng is not None:
					odds['biaozhu'] = zhusheng
					odds['biaoping'] = ping
					odds['biaoke'] = kesheng
					clear2 = True
			o3_first_tds = o3_first.xpath(u".//td[contains(@class,'tr-data')]")
			if (len(o3_first_tds) ==3 and not clear3):
				da = o3_first_tds[0].xpath(u".//span/text()").re_first('(.+)')
				dapan = o3_first_tds[1].xpath(u".//text()").re_first('(.+)')
				xiao = o3_first_tds[2].xpath(u".//span/text()").re_first('(.+)')
				if da is not None and dapan is not None and xiao is not None:
					odds['da'] = da	
					odds['dapan'] = dapan
					odds['xiao'] = xiao
					clear3 = True
			if clear1 and clear2 and clear3:
				break
		yield odds
			
			
			
			 


