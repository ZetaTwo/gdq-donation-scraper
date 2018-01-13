# Games Done Quick Donation Tracker Scraper

## Installation

```
pip install -r requirements.txt
```

## Usage

You don't have to run the scraper unless you want to add new data. The scraped data is already checked in as donations.db
```
python scraper.py
```

Then remove outliers according to data_fixes.txt and create data.csv by running.
```
python convert.py
```

Finally, view index.html, for example by using
```
python -m SimpleHTTPServer
```
