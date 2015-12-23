# MongoDB Scaper

So accordingly to Shodan, there are more than 30k Mongodb instances publicly available, running on the standard port. Many of them are running with default settings (ie no authentication required).

What if we start scraping them all and dump the data?

### Requirements
```
pip install pymongo
pip install colorlog
```

### Usage
```
python mongodb-scaper.py
```

You can supply a comma separate list of IPs as an additional argument `--skip` to manually check some IPs as processed and thus exlude them from the stack
```
python mongodb-scraper.py --skip "123.123.123.123,123.456.789.123"
```
