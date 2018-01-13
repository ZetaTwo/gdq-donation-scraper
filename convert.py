#!/usr/bin/env python
from common import setup_db, close_db, cur_db
from datetime import datetime, timedelta, time, date
import csv

db = setup_db()

TIMESTEP = timedelta(minutes=5)
STALETIME = timedelta(days=1, hours=6)
MIN_TICKS = 50

with cur_db(db) as cur:
	events = cur.execute("SELECT id, name, slug FROM events LIMIT 200;").fetchall()
event_slugs = [e[2] for e in events]
event_data = {}

for event in events:
	with cur_db(db) as cur:
		ticks = []
		tickamount = 0
		startime = None
		lastactivity = timedelta()
		nexttick = timedelta() + TIMESTEP

		cur.execute("SELECT datetime, amount FROM donations WHERE event_id = ? ORDER BY datetime ASC", (event[0],))
		for timestamp, amount in cur:
			timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
			if not startime:
				startime = timestamp
			delta = timestamp - startime

			if delta - lastactivity > STALETIME and len(ticks) > MIN_TICKS:
				print('Event "%s" stale after %d ticks' % (event[0], len(ticks)))
				print(lastactivity, nexttick, timestamp)
				print(delta, nexttick - lastactivity)
				break

			while delta > nexttick:
				ticks.append(tickamount)
				nexttick += TIMESTEP

			tickamount += amount

			lastactivity = delta
		ticks.append(tickamount)

	event_data[event[2]] = ticks

event_slugs = []
values = []
maxlen = max([len(t) for t in event_data.values()])
print(maxlen)
for event_slug, ticks in event_data.items():
	event_slugs.append(event_slug)
	#extended_ticks = ticks + [ticks[-1]]*(maxlen - len(ticks))
	extended_ticks = ticks + [None]*(maxlen - len(ticks))
	assert(len(extended_ticks) == maxlen)
	values.append(extended_ticks)

with open('data.csv', 'wb') as fout:	
	csvwriter = csv.writer(fout)
	headers = ['date'] + event_slugs
	csvwriter.writerow(headers)
	t = datetime(2010, 1, 1)
	for row in zip(*values):
		csvwriter.writerow([t.strftime('%Y-%m-%d %H:%M')] + list(row))
		t += TIMESTEP




close_db(db)