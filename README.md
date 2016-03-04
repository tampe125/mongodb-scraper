# MongoDB Scraper

So accordingly to Shodan, there are more than 30k Mongodb instances publicly available, running on the standard port. Many of them are running with default settings (ie no authentication required).

What if we start scraping them all and dump the passwords?

### Requirements
```
pip install pymongo
pip install colorlog
```

### Usage
First of all create a `data.json` file, including a JSON encoded array of IPs:
```
["123.456.789", "987.654.321"]
```
If you have downloaded a report from Shodan, you can easily parse it using the script file `parse_data.py`.  
Then simply run the scraper using the following command:
```
python mongodb-scaper.py
```

You can supply a comma separate list of IPs as an additional argument `--skip` to manually check some IPs as processed and thus exlude them from the stack
```
python mongodb-scraper.py --skip "123.123.123,123.456.789"
```

### Get alerts on juicy results
If you want to get an email when you find some BIG DUMP (default when there are more than 1M of rows), simply copy the `settings-dist.json` file and rename it to `settings.json`, filling all the fields.
