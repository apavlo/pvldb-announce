import os
import sys

## ==============================================
## CONFIGURATION
## ==============================================

RSS_TITLE = 'PVLDB Paper Announcements'
RSS_AUTHOR = {'name':'Andy Pavlo','email':'pavlo@cs.cmu.edu'}
RSS_SUBTITLE = 'Digest for PVLDB papers generated by the Carnegie Mellon Database Group'
RSS_FILE = "pvldb-rss.xml"
RSS_URL = "https://db.cs.cmu.edu/files/" + RSS_FILE

START_URL = "https://vldb.org/pvldb/vol%d-volume-info/"
BASE_URL = "https://www.vldb.org"
#BASE_URL = os.path.join(HOMEPAGE_URL, "/pvldb/")

# DB_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "pvldb.db")

POST_SLEEP_TIME = 1200 # seconds
POST_MAX_NUM_CHARS = {
    "twitter": 250,
    "mastodon": 1024
}

SKIP = set([ "vol%d.html" % x for x in range(1, 5) ])
