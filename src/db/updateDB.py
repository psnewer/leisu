# -*- coding: utf-8 -*-
import json
import sqlite3
import os
import re 
import logging
import logging.config
import collections 
import gflags
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import codecs
import operator
import sys
import string
from dateutil.parser import parse

gflags.DEFINE_bool('ignore', False, 'ignore new name')

matchFileDirectory = '/Users/miller/Documents/workspace/leisu/leisu/matches.json'
idsDirectory = '/Users/miller/Documents/workspace/leisu/src/db/ids.json'
tips_file = '/Users/miller/Documents/workspace/leisu/src/db/name_tips.json'
db = '/Users/miller/Desktop/soccer.db'
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute('''drop Table TMatch''')
cur.execute('''CREATE TABLE `TMatch` (`id` INTEGER PRIMARY KEY AUTOINCREMENT, 'continent_id' INTEGER, `country_id` INTEGER, `league_id` INTEGER, `season`    TEXT, `serryid`    TEXT, `serryname`    TEXT, `stage`    INTEGER, `date` TEXT, 'home_team_id' INTEGER, 'away_team_id' INTEGER, FOREIGN KEY(`continent_id`) REFERENCES `Continent`(`id`),FOREIGN KEY(`country_id`) REFERENCES `Country`(`id`),FOREIGN KEY(`league_id`) REFERENCES `League`(`id`),FOREIGN KEY(`home_team_id`) REFERENCES `Team`(`id`),FOREIGN KEY(`away_team_id`) REFERENCES `Team`(`id`), CONSTRAINT match_unique UNIQUE (league_id, date, home_team_id, away_team_id))''')

def check(filepath, idspath):
	logging.config.fileConfig("/Users/miller/Documents/workspace/leisu/log/logging.conf")
	logdb = logging.getLogger("db")
	data = open(filepath)
	idsdata = open(idspath)
	ids = json.load(idsdata)
	signal = False
	for row in data:
		rowdata = json.loads(row)
		continent = rowdata['continent']
		country = rowdata['country']
		league = rowdata['league']
		season = rowdata['season']
		serryid = rowdata['serryid']
		serryname = rowdata['serryname']
		stage = rowdata['stage']
		date = rowdata['date']
		home_team = rowdata['home_team']
		away_team = rowdata['away_team']
		home_goal= rowdata['home_goal']
		away_goal = rowdata['away_goal']
		if continent not in ids:
			logdb.error("new continent id : " + continet)
			signal = True
		if country not in ids:
			logdb.error("new country id : " + country)
			signal = True
		if league not in ids:
			logdb.error("new league id : " + league)
			signal = True
		if home_team not in ids:
			logdb.error("new home_team id : " + home_team)
			signal = True
		if away_team not in ids:
			logdb.error("new away_team id : " + away_team)
			signal = True
	idsdata.close()
	data.close()
	if(signal):
		if gflags.FLAGS.ignore:
			return
		else:
			sys.exit(0)

def saveMatch(filepath, idspath):
	data = open(filepath)
	idsdata = codecs.open(idspath,'r+',encoding='utf-8')
	ids = json.load(idsdata)
	nametip = codecs.open(tips_file,'r+',encoding='utf-8')
	name_tips = json.load(nametip)
	for row in data:
		continent_id = 0
		country_id = 0
		league_id = 0
		home_team_id = 0
		away_team_id = 0
		rowdata = json.loads(row)
		continent = rowdata['continent']
		country = rowdata['country']
		league = rowdata['league']
		season = rowdata['season']
		stage = rowdata['stage']
		serryid = rowdata['serryid']
		serryname = rowdata['serryname']
		date = rowdata['date']
		home_team = rowdata['home_team']
		away_team = rowdata['away_team']
		home_goal= rowdata['home_goal']
		away_goal = rowdata['away_goal']
		if serryname is None:
			serryname = 'default'
		elif serryname in name_tips:
			serryname = name_tips[serryname]
		if continent not in ids and gflags.FLAGS.ignore:
			cur.execute('''INSERT OR IGNORE INTO Continent (name) VALUES ( ? )''', (continent, ) )
			cur.execute('SELECT id FROM Continent WHERE name = ? ', (continent, ))
			continent_id = cur.fetchone()[0]
			ids[continent] = continent_id
		else:
			continent_id = ids[continent]
		if country not in ids and gflags.FLAGS.ignore:
			cur.execute('''INSERT OR IGNORE INTO Country (name,continent_id) VALUES ( ?, ? )''', (country,continent_id, ) )
			cur.execute('SELECT id FROM Country WHERE name = ? ', (country, ))
			country_id = cur.fetchone()[0]
			ids[country] = country_id
		else:
			country_id = ids[country]
		if league not in ids and gflags.FLAGS.ignore:
			cur.execute('''INSERT OR IGNORE INTO League (name,country_id) VALUES ( ?, ? )''', (league,country_id, ) )
			cur.execute('SELECT id FROM League WHERE name = ? ', (league, ))
			league_id = cur.fetchone()[0]
			ids[league] = league_id
		else:
			league_id = ids[league]
		if home_team not in ids and gflags.FLAGS.ignore:
			cur.execute('''INSERT OR IGNORE INTO Team (name) VALUES ( ? )''', (home_team, ) )
			cur.execute('SELECT id FROM Team WHERE name = ? ', (home_team, ))
			home_team_id = cur.fetchone()[0]
			ids[home_team] = home_team_id
		else:
			home_team_id = ids[home_team]
		if away_team not in ids and gflags.FLAGS.ignore:
			cur.execute('''INSERT OR IGNORE INTO Team (name) VALUES ( ? )''', (away_team, ) )
			cur.execute('SELECT id FROM Team WHERE name = ? ', (away_team, ))
			away_team_id = cur.fetchone()[0]
			ids[away_team] = away_team_id
		else:
			away_team_id = ids[away_team]
		if (home_goal == -1):
			cur.execute('''INSERT OR IGNORE INTO TMatch (continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (continent_id, country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,))
		else:
			cur.execute('''INSERT OR IGNORE INTO Match (continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (continent_id, country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal,))
	if (gflags.FLAGS.ignore):
		idsdata.seek(0,os.SEEK_SET)
		json.dump(ids,idsdata, ensure_ascii=False)
	idsdata.close()
	data.close()
	conn.commit()
	conn.close()

if __name__ == "__main__":
	gflags.FLAGS(sys.argv)
	check(matchFileDirectory,idsDirectory)
	saveMatch(matchFileDirectory,idsDirectory)
