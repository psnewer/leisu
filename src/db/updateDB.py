import json
import sqlite3
import os
import re 
import time
import logging
import logging.config
import xml.etree.ElementTree as ET
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

matchFileDirectory = '/Users/miller/Documents/workspace/leisu/leisu/items.json'
idsDirectory = '/Users/miller/Documents/workspace/leisu/src/db/ids.json'
db = '/Users/miller/Desktop/soccer.db'
errorFile = '/Users/hugomathien/Documents/workspace/footballdata/match_error.txt'
conn = sqlite3.connect(db)
cur = conn.cursor()

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
		serry = rowdata['serry']
		stage = rowdata['stage']
		date = rowdata['date']
		home_team = rowdata['home_team']
		away_team = rowdata['away_team']
		home_goal= rowdata['home_goal']
		away_goal = rowdata['away_goal']
		if continent not in ids:
			logdb.error("new continent id : " + continet)
			signal = True
			break
		if country not in ids:
			logdb.error("new country id : " + country)
			signal = True
			break
		if league not in ids:
			logdb.error("new league id : " + league)
			signal = True
			break
		if home_team not in ids:
			logdb.error("new home_team id : " + home_team)
			signal = True
			break
		if away_team not in ids:
			logdb.error("new away_team id : " + away_team)
			signal = True
			break
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
	continent_id = 0
	country_id = 0
	league_id = 0
	home_team_id = 0
	away_team_id = 0
	for row in data:
		rowdata = json.loads(row)
		continent = rowdata['continent']
		country = rowdata['country']
		league = rowdata['league']
		season = rowdata['season']
		serry = rowdata['serry']
		stage = rowdata['stage']
		date = rowdata['date']
		home_team = rowdata['home_team']
		away_team = rowdata['away_team']
		home_goal= rowdata['home_goal']
		away_goal = rowdata['away_goal']
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
		cur.execute('''INSERT OR IGNORE INTO Match (continent_id,country_id,league_id,season,serry,date,stage,home_team_id,away_team_id,home_goal,away_goal) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (continent_id, country_id,league_id,season,serry,date,stage,home_team_id,away_team_id,home_goal,away_goal,))
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
