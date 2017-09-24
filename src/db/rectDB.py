import json
import sqlite3
import os
import re 
import time
import logging
import logging.config
import xml.etree.ElementTree as ET
import collections 
import codecs
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import operator
import sys
import string
from dateutil.parser import parse

warnningFileDirectory = '/Users/miller/Documents/workspace/leisu/log/logww.log'
idsDirectory = '/Users/miller/Documents/workspace/leisu/src/db/ids.json'
db = '/Users/miller/Desktop/soccer.db'
conn = sqlite3.connect(db)
cur = conn.cursor()

def checkError():
	logging.config.fileConfig("/Users/miller/Documents/workspace/leisu/log/logging.conf")
	logrect = logging.getLogger("rect")
	data = codecs.open(warnningFileDirectory,'r',encoding='utf-8')
	idsdata = codecs.open(idsDirectory,'r',encoding='utf-8')
	ids = json.load(idsdata)
	for row in data:
		rowdata = re.search('WARNING (.+)',row.strip()).group(1)
		elems = rowdata.split(' ')
		league = elems[2]
		season = elems[3]
		serry = elems[4]
		stage = elems[5]
		home_team = elems[6]
		away_team = elems[7]
		if (len(elems) < 8):
			logrect.error(rowdata)
			continue
		if (league not in ids):
			logrect.error(rowdata)
			continue
		else:
			league_id = ids[league]
		if (home_team not in ids):
			logrect.error(rowdata)
			continue
		else:
			home_team_id = ids[home_team]
		if (away_team not in ids):
			logrect.error(rowdata)
			continue
		else:
			away_team_id = ids[away_team]
		cur.execute('SELECT * FROM Match WHERE league_id = ? and season = ? and serry = ? and stage = ? and home_team_id = ? and away_team_id = ?', (league_id,season,serry,stage,home_team_id,away_team_id, ))
		items = cur.fetchone()
		if (items is None):
			logrect.error(rowdata)
		else:
			print '7777'
	idsdata.close()
	data.close()
	conn.close()

if __name__ == "__main__":
	checkError()
