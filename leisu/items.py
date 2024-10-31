# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Match(scrapy.Item):
	continent = scrapy.Field()
	country = scrapy.Field()
	league = scrapy.Field()
	season = scrapy.Field()
	serryid = scrapy.Field()
	serryname = scrapy.Field()
	stage = scrapy.Field()
	match_id = scrapy.Field()
	date = scrapy.Field()
	home_team = scrapy.Field()
	away_team = scrapy.Field()
	score = scrapy.Field()
	half_score = scrapy.Field()
	procedure = scrapy.Field()
