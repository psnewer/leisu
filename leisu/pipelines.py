# -*- coding: utf-8 -*-

import json
import os
import sys
import time
import collections
from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter
from leisu.items import Match

class CustomJsonLinesItemExporter(JsonLinesItemExporter):  
    def __init__(self, file, **kwargs):  
        super(CustomJsonLinesItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)

class MatchPipeline(object):
	def __init__(self):
		self.file = open('./leisu/matches.json', 'wb')
		self.exporter = CustomJsonLinesItemExporter(self.file)
		self.exporter.fields_to_export = [
				'continent',
				'country',
				'league',
				'season',
				'stage',
				'serryid',
				'serryname',
				'match_id',
				'date',
				'home_team',
				'away_team',
				'home_goal',
				'away_goal',
				'procedure']

	def process_item(self, item, spider):
		if spider.name == 'sl':
			self.exporter.export_item(item)
		return item

import sqlite3
from scrapy.exceptions import DropItem

class SQLitePipeline:
    def __init__(self):
        # 连接到 SQLite 数据库（如果数据库不存在，会自动创建）
        self.connection = sqlite3.connect('./src/db/matches.db')
        self.cursor = self.connection.cursor()
        # 创建表格
        self.create_table()

    def create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                continent TEXT,
                country TEXT,
                league TEXT,
                season TEXT,
                serryid TEXT,
                serryname TEXT,
                stage TEXT,
                match_id TEXT PRIMARY KEY,
                date TEXT,
                home_team TEXT,
                away_team TEXT,
                score TEXT,
                half_score TEXT,
                procedure TEXT
            )
        ''')
        self.connection.commit()

    def process_item(self, item, spider):
        # 将 item 插入到数据库
        self.cursor.execute('''
            INSERT OR REPLACE INTO matches (continent, country, league, season, serryid, serryname, stage, match_id, date, home_team, away_team, score, half_score, procedure) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item['continent'],
            item['country'],
            item['league'],
            item['season'],
            item['serryid'],
            item['serryname'],
            item['stage'],
            item['match_id'],
            item['date'],
            item['home_team'],
            item['away_team'],
            item['score'],
            item['half_score'],
            item['procedure']
        ))
        self.connection.commit()
        return item

    def close_spider(self, spider):
        # 关闭数据库连接
        self.connection.close()

