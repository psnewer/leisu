# -*- coding: utf-8 -*-
import json
import sqlite3
import os
import re
import xml.etree.ElementTree as ET
import collections 
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import codecs
import operator
import sys
import string
from dateutil.parser import parse

db = '/Users/miller/Desktop/soccer.db'
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='Match_back'")
if cur.fetchone()[0] == 0:
	cur.execute("CREATE TABLE Match_back AS SELECT * FROM Match")
	conn.commit()

def refresh_db(date_1,date_2):
#	cur.execute('''drop table TMatch''')
#	cur.executescript('''CREATE TABLE `TMatch` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, 'continent_id' INTEGER, `country_id` INTEGER, `league_id` INTEGER, `season`    TEXT, `serryid`    TEXT, `serryname`    TEXT, `stage`    INTEGER, `date` TEXT, 'home_team_id' INTEGER, 'away_team_id' INTEGER, 'home_goal' INTEGER, 'away_goal' INTEGER, FOREIGN KEY(`continent_id`) REFERENCES `Continent`(`id`),FOREIGN KEY(`country_id`) REFERENCES `Country`(`id`),FOREIGN KEY(`league_id`) REFERENCES `League`(`id`),FOREIGN KEY(`home_team_id`) REFERENCES `Team`(`id`),FOREIGN KEY(`away_team_id`) REFERENCES `Team`(`id`), CONSTRAINT match_unique UNIQUE (league_id, season, date, home_team_id, away_team_id)); CREATE INDEX tmatch_index on TMatch(league_id, season, serryid, serryname, stage, date, home_team_id, away_team_id)''')
#	cur.execute("SELECT continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal FROM Match WHERE date >= '%s' and date < '%s'"%(date_1,date_2))
#	rows = cur.fetchall()
#	for row in rows:
#		cur.execute('''INSERT OR IGNORE INTO TMatch (continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]))
	cur.execute("SELECT continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal FROM Match WHERE date >= '%s' and date < '%s'"%(date_1,date_2))
	rows = cur.fetchall()
	for row in rows:
		cur.execute('''INSERT OR IGNORE INTO TMatch (continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
	cur.execute("DELETE FROM Match WHERE date >= '%s'"%(date_1))
	conn.commit()

if __name__ == '__main__':
	date_1 = sys.argv[1]
	date_2 = sys.argv[2]
	refresh_db(date_1,date_2)
