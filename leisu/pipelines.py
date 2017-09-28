# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import collections
from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter

class CustomJsonLinesItemExporter(JsonLinesItemExporter):  
    def __init__(self, file, **kwargs):  
        super(CustomJsonLinesItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)

class JsonPipeline(object):
	def __init__(self):
		self.file = open('/Users/miller/Documents/workspace/leisu/leisu/items.json', 'wb')
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
		self.exporter.export_item(item)
		return item

#    def spider_closed(self, spider):
#        self.exporter.finish_exporting()
#        self.file.close()
#		today=time.strftime("%Y%m%d", time.localtime())
#		os.system('mkdir /Users/miller/Documents/workspace/leisu/log/"%s"'%today)
#		os.system('mv /Users/miller/Documents/workspace/leisu/log/* /Users/miller/Documents/workspace/leisu/log/"%s"'%today)
#
