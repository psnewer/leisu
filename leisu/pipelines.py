# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import collections
from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter
from leisu.items import Match,Odds

class CustomJsonLinesItemExporter(JsonLinesItemExporter):  
    def __init__(self, file, **kwargs):  
        super(CustomJsonLinesItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)

class MatchPipeline(object):
	def __init__(self):
		self.file = open('/Users/miller/Documents/workspace/leisu/leisu/matches.json', 'wb')
		self.exporter = CustomJsonLinesItemExporter(self.file)
		self.exporter.fields_to_export = [
				'continent',
				'country',
				'league',
				'season',
				'stage',
				'serryid',
				'serryname',
				'date',
				'home_team',
				'away_team',
				'home_goal',
				'away_goal']

	def process_item(self, item, spider):
		if spider.name == 'sl':
			self.exporter.export_item(item)
		return item

class OddsPipeline(object):
	def __init__(self):
		self.file = open('/Users/miller/Documents/workspace/leisu/leisu/odds.json', 'wb')
		self.exporter = CustomJsonLinesItemExporter(self.file)
		self.exporter.fields_to_export = [
				'league',
				'season',
				'date',
				'home_team',
				'away_team',
				'rangzhu',
				'rangpan',
				'rangke',
				'biaozhu',
				'biaoping',
				'biaoke',
				'da',
				'dapan',
				'xiao']

	def process_item(self, item, spider):
		if spider.name == 'odds':
			self.exporter.export_item(item)
		return item
