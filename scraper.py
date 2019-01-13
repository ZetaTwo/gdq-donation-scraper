#!/usr/bin/env python
from requests import Request, Session
import os
import re
import errno
from datetime import datetime
from lxml import html
import logging
import tqdm
import time
from common import setup_db, close_db, cur_db
logging.basicConfig(level=logging.INFO)

TRACKER_URL = 'https://gamesdonequick.com/tracker'
EVENTS_URL = TRACKER_URL + '/donations/'
DONATIONS_URL = TRACKER_URL + '/donations/%s'

ONGOING_EVENTS = ['agdq2019']
ONLY_EVENTS = ['agdq2018', 'sgdq2018', 'GDQX2018', 'agdq2019']
ROWS_PER_PAGE = 50
RATE_LIMIT_MESSAGE = 'You are being rate limited'
SLEEP_AMOUNT = 3

def assure_directories(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def setup_cache():
    assure_directories('.cache')


class Event(object):
    def __init__(self, name, slug):
        self.name = name
        self.slug = slug

class Donation(object):
    def __init__(self, id, name, donor_id, datetime, amount):
        self.id = id
        self.name = name
        self.donor_id = donor_id
        self.datetime = datetime
        self.amount = amount

def fetch_cached(file):
    try:
        with open(os.path.join('.cache', file), 'rb') as fin:
            return fin.read()
    except:
        return False

def store_cache(file, data):
    cache_path = os.path.join('.cache', file)
    try:
        with open(cache_path, 'wb') as fout:
            fout.write(data)
    except:
        os.remove(cache_path)
        raise

    return True

def fetch_url_cached(url, force=False, **kwargs):
    request = Request('GET', url, **kwargs).prepare()
    url = request.url
    
    cache_path = 'url_%s.html' % url.encode('hex')
    cached = fetch_cached(cache_path)
    
    if not cached or force:
        logging.debug('Fetching URL: %s', url)
        time.sleep(SLEEP_AMOUNT)
        s = Session()
        data = s.send(request,stream=True).raw.read()
        if RATE_LIMIT_MESSAGE in data:
            raise RuntimeError('Rate limited when requesting "%s"' % url)
        store_cache(cache_path, data)
    else:
        logging.debug('Reading URL from cache: %s', url)
        data = cached

    return data

def fetch_events():
    events_html = fetch_url_cached(EVENTS_URL, force=True)
    dom = html.document_fromstring(events_html)
    tracker_links = [(l[0].text_content(), l[2].split('/')[-1]) for l in dom.iterlinks() if l[1] == 'href' and l[2].startswith('/tracker/index/')]
    events = [x for x in tracker_links if len(x[1]) > 0]
    if len(ONLY_EVENTS) > 0:
        events = [e for e in events if e[1] in ONLY_EVENTS]
    return events

def fetch_event_page(event, page, force=False):
    return fetch_url_cached(DONATIONS_URL % event, force=force, params={'page': page, 'sort':'time', 'order':1})

def extract_dom_row(row):
    name = row[0].text_content().strip()
    donor_id = row[0].xpath('a/@href')
    if len(donor_id) > 0:
        donor_id = int(donor_id[0].split('/')[3])
    else:
        donor_id = None

    datetime_str = row[1].text_content().strip()
    timestamp = datetime.strptime(datetime_str, '%m/%d/%Y %H:%M:%S +0000') #01/05/2010 03:10:14 +0000
    
    amount_match = re.match('\$([0-9., ]+)', row[2].text_content().strip())
    amount = int(amount_match.group(1).replace(',','').replace('.',''))
    id = int(row[2].xpath('a/@href')[0].split('/')[-1])

    return Donation(id, name, donor_id, timestamp, amount)

def process_event_page(data):
    dom = html.document_fromstring(data)
    table_rows = [tr.xpath('td') for tr in dom.xpath('//table//tr')]
    table_rows = [extract_dom_row(tr) for tr in table_rows if len(tr) == 4]
    
    try:
        num_pages = int(dom.xpath('//label[@for="sort"]')[0].text_content().strip().split(' ')[-1])
    except:
        logging.error('Could not find max pages')
        raise
    
    return table_rows, num_pages

def store_event(db, event_name, event_slug):
    logging.debug('Storing event "%s"', event_slug)
    with cur_db(db) as cur:
        cur.execute("INSERT OR IGNORE INTO events (name, slug) VALUES (?, ?)", (event_name, event_slug))
        event_id = cur.execute("SELECT id FROM events WHERE slug = ?", (event_slug,)).fetchone()[0]
        logging.debug('Event with name "%s" inserted or retrieved as id "%s"', event_slug, event_id)
    
    return event_id

def store_donation(db, event_id, donation):
    logging.debug('Storing donation of "$c%d"  by "%s"', donation.amount, donation.name)
    with cur_db(db) as cur:
        cur.execute(
            "INSERT OR IGNORE INTO donations (id, event_id, name, donor_id, datetime, amount) VALUES (?, ?, ?, ?, ?, ?)",
            (donation.id, event_id, donation.name, donation.donor_id, donation.datetime, donation.amount)
        )

if __name__ == "__main__":
    setup_cache()
    db = setup_db()

    events = fetch_events()
    for event_name, event_slug in events:
        logging.info('Processing event "%s"', event_slug)

        event_id = store_event(db, event_name, event_slug)

        event_page = fetch_event_page(event_slug, 1, force=True)

        _, num_pages = process_event_page(event_page)
        estimated_num_rows = num_pages * ROWS_PER_PAGE

        if event_slug in ONGOING_EVENTS:
            logging.info('Event "%s" is ongoing, skipping last page to avoid data corruption', event_slug)
            num_pages -= 1

        for page in tqdm.trange(num_pages):
            event_page = fetch_event_page(event_slug, page+1)
            try:
                event_rows, _ = process_event_page(event_page)
            except:
                logging.error('Error in event "%s" page "%d"', event_slug, page+1)
                raise

            for donation in event_rows:
                store_donation(db, event_id, donation)

        db.commit()


    close_db(db)
