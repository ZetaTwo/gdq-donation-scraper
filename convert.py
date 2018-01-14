#!/usr/bin/env python
from common import setup_db, close_db, cur_db
from datetime import datetime, timedelta, time, date
import csv

db = setup_db()

TIMESTEP = timedelta(minutes=5)
STALETIME = timedelta(days=1, hours=6)

STARTTIMES = {
	'cgdq': 	'2010-01-02 00:00:00',
	'agdq2011':	'2011-01-06 21:00:00',
	'jrdq':		'2011-04-07 19:30:00',
	'sgdq2011':	'2011-08-04 15:00:00',
	'agdq2012':	'2012-01-04 21:30:00',
	'sgdq2012':	'2012-05-24 19:00:00',
	'spook':	'2012-10-26 18:00:00',
	'agdq2013':	'2013-01-06 16:00:00',
	'sgdq2013':	'2013-07-25 14:00:00',
	'agdq2014':	'2014-01-05 15:00:00',
	'sgdq2014':	'2014-06-22 16:00:00',
	'agdq2015':	'2015-01-04 14:00:00',
	'sgdq2015':	'2015-07-26 16:00:00',
	'agdq2016':	'2016-01-03 14:00:00',
	'sgdq2016':	'2016-07-03 14:00:00',
	'agdq2017':	'2017-01-08 15:00:00',
	'sgdq2017':	'2017-07-02 15:00:00',
	'hrdq':		'2017-09-01 16:00:00',
	'agdq2018':	'2018-01-07 16:00:00'
}

with cur_db(db) as cur:
	events = cur.execute("SELECT id, name, slug FROM events LIMIT 200;").fetchall()
event_slugs = [e[2] for e in events]
event_data = {}

for event in events:
	event_id, _, event_slug = event
	with cur_db(db) as cur:
		ticks = []
		tickamount = 0
		lastactivity = timedelta()
		nexttick = timedelta() + TIMESTEP
		if event_slug in STARTTIMES:
			startime = datetime.strptime(STARTTIMES[event_slug], '%Y-%m-%d %H:%M:%S')
		else:
			startime = None


		cur.execute("SELECT datetime, amount FROM donations WHERE event_id = ? ORDER BY datetime ASC", (event_id,))
		for timestamp, amount in cur:
			timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
			if not startime:
				startime = timestamp
			delta = timestamp - startime

			if delta - lastactivity > STALETIME:
				print('Event "%s" stale after %d ticks' % (event_id, len(ticks)))
				print(lastactivity, nexttick, timestamp)
				print(delta, nexttick - lastactivity)
				break

			while delta > nexttick:
				ticks.append(tickamount)
				nexttick += TIMESTEP

			tickamount += amount
			if delta > lastactivity:
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
