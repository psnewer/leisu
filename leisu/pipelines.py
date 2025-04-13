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

class ATPPipeline:
    def open_spider(self, spider):
        """爬虫启动时，连接 SQLite 数据库并创建表"""
        self.conn = sqlite3.connect("./src/db/tennis.db")  # 连接数据库
        self.cursor = self.conn.cursor()

        # 创建 tennis 表（如果不存在）
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tennis (
                match_id TEXT PRIMARY KEY,  -- 确保 match_id 唯一
                tour TEXT,
                field TEXT,
                season TEXT,
                date TEXT,
                home TEXT,
                away TEXT,
                home_score TEXT,   
                away_score TEXT,        
                sets TEXT  -- 存储 JSON 格式的 sets 数据
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        """处理爬取到的数据并存入数据库"""
        self.cursor.execute("""
            INSERT INTO tennis (match_id, tour, field, season, date, home, away, home_score, away_score, sets)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                tour = excluded.tour,
                field = excluded.field,
                season = excluded.season,
                date = excluded.date,
                home = excluded.home,
                away = excluded.away,
                home_score = excluded.home_score,
                away_score = excluded.away_score,
                sets = excluded.sets
        """, (
            item["match_id"], 
            item["tour"], 
            item["field"], 
            item["season"], 
            item["date"], 
            item["home"], 
            item["away"], 
            item["home_score"], 
            item["away_score"], 
            json.dumps(item["sets"], ensure_ascii=False)  # 转换 sets 为 JSON 字符串存储
        ))
        self.conn.commit()

        return item

    def close_spider(self, spider):
        """爬虫结束时，关闭数据库连接"""
        self.conn.close()

class ATPRanking:
    def open_spider(self, spider):
        """爬虫启动时，连接 SQLite 数据库并创建表"""
        self.conn = sqlite3.connect("./src/db/tennis.db")  # 连接数据库
        self.cursor = self.conn.cursor()

        # 创建 ranking 表（如果不存在）
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ranking (
                player TEXT PRIMARY KEY,  -- 选手姓名作为主键
                rank INTEGER NOT NULL,    -- 当前排名
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP  -- 最后更新时间
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        """处理爬取到的数据并存入数据库"""
        self.cursor.execute("""
            INSERT OR REPLACE INTO ranking (rank, player)
            VALUES (?, ?)
        """, (
            item["rank"], 
            item["player"]
        ))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        """爬虫结束时，关闭数据库连接"""
        self.conn.close()