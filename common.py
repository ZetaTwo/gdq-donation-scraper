#!/usr/bin/env python
import sqlite3
from contextlib import closing

def setup_db():
    db = sqlite3.connect('donations.db')
    with closing(db.cursor()) as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                slug TEXT UNIQUE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS donations (
                id INTEGER PRIMARY KEY,
                event_id INTEGER,
                name TEXT,
                donor_id INT,
                datetime DATETIME,
                amount INTEGER
            );
        """)
    return db

def cur_db(db):
    return closing(db.cursor())

def close_db(db):
    db.commit()
    db.close()
