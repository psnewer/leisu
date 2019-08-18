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

matchFileDirectory = '../../leisu/matches.json'
idsDirectory = './ids.json'
tips_file = './name_tips.json'
db = './soccer.db'
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute('''DELETE FROM TMatch''')

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
		home_odds = rowdata['home_odds']
		away_odds = rowdata['away_odds']
		pan = rowdata['pan']
		if serryname is None:
			serryname = 'default'
		elif serryname in name_tips:
			serryname = name_tips[serryname]
		serryname = re.sub("[()-/ ]+","",serryname)
		if league in name_tips:
			league = name_tips[league]
		if continent not in ids:
			cur.execute('''Replace INTO Continent (name) VALUES ( ? )''', (continent, ) )
			cur.execute('SELECT id FROM Continent WHERE name = ? ', (continent, ))
			continent_id = cur.fetchone()[0]
			ids[continent] = continent_id
		else:
			continent_id = ids[continent]
		if country not in ids:
			cur.execute('''Replace INTO Country (name,continent_id) VALUES ( ?, ? )''', (country,continent_id, ) )
			cur.execute('SELECT id FROM Country WHERE name = ? ', (country, ))
			country_id = cur.fetchone()[0]
			ids[country] = country_id
		else:
			country_id = ids[country]
		if league not in ids:
			cur.execute('''Replace INTO League (name,country_id) VALUES ( ?, ? )''', (league,country_id, ) )
			cur.execute('SELECT id FROM League WHERE name = ? ', (league, ))
			league_id = cur.fetchone()[0]
			ids[league] = league_id
		else:
			league_id = ids[league]
		if home_team not in ids:
			cur.execute('''Replace INTO Team (name) VALUES ( ? )''', (home_team, ) )
			cur.execute('SELECT id FROM Team WHERE name = ? ', (home_team, ))
			home_team_id = cur.fetchone()[0]
			ids[home_team] = home_team_id
		else:
			home_team_id = ids[home_team]
		if away_team not in ids:
			cur.execute('''Replace INTO Team (name) VALUES ( ? )''', (away_team, ) )
			cur.execute('SELECT id FROM Team WHERE name = ? ', (away_team, ))
			away_team_id = cur.fetchone()[0]
			ids[away_team] = away_team_id
		else:
			away_team_id = ids[away_team]
		if (home_goal == -1):
			cur.execute('''Replace INTO TMatch (continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_odds,away_odds,pan) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (continent_id, country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_odds,away_odds,pan,))
		else:
			cur.execute('''Replace INTO Match (continent_id,country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal,home_odds,away_odds,pan) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (continent_id, country_id,league_id,season,serryid,serryname,date,stage,home_team_id,away_team_id,home_goal,away_goal,home_odds,away_odds,pan,))
	idsdata.seek(0,os.SEEK_SET)
	json.dump(ids,idsdata, ensure_ascii=False)
	idsdata.close()
	data.close()
	conn.commit()

def updateTMatch():
	now = time.strftime("%Y%m%d%H%M", time.localtime())
	cur.execute("DELETE FROM TMatch WHERE date <= '%s'"%(now))
	conn.commit()

saveMatch(matchFileDirectory,idsDirectory)
#updateTMatch()
conn.close()
