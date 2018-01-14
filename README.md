# Games Done Quick Donation Tracker Scraper

## Installation

```
pip install -r requirements.txt
```

## Usage

You don't have to run the scraper unless you want to add new data.
The scraped data is already checked in as donations.db
```
python scraper.py
```

Then convert the SQLite database into an appropriate CSV file for display.
```
python convert.py
```

Finally, view index.html, for example by using
```
python -m SimpleHTTPServer
```

## Demo

You can see the results on [the GitHub page](https://zetatwo.github.io/gdq-donation-scraper).
