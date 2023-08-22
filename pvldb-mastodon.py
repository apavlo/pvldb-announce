#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import urllib
import logging
import pytz
import time
import argparse
import sqlite3
from mastodon import Mastodon
from datetime import datetime
from datetime import tzinfo
from pprint import pprint, pformat

from config import *

## ==============================================
## LOGGING
## ==============================================
LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)



## ==============================================
## postMastodon
## ==============================================
def postMastodon(args, db, paper):
    LOG.info("Posting paper '%s' to Mastodon!", paper["title"])

    api = Mastodon(
        access_token=args["mastodon_api_key"],
        api_base_url=args["mastodon_url"]
    )

    post = u"Vol:%(volume)d No:%(number)d → %(title)s" % paper
    if len(post)+24 > MASTODON_NUM_CHARS:
        remaining = MASTODON_NUM_CHARS - (len(post)+24)
        post = post[:remaining-3] + u"..."
    post += " " + paper["link"]
    
    LOG.debug("%s [Length=%d]", post, len(post))

    status = api.status_post(post)
    LOG.info("Wrote post to %s [status=%s]", args["mastodon_url"], str(status))
    
    cur = db.cursor()
    sql = "UPDATE papers SET mastodon = 1 WHERE link = ?"
    cur.execute(sql, (paper["link"], ))
    db.commit()
    
## DEF


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='PVLDB Announcements Script')
    aparser.add_argument('dbpath', help='Database Path')
    aparser.add_argument("--debug", action='store_true')
    aparser.add_argument('--limit', type=int, help='Number of papers to announce before stopping')
    aparser.add_argument('--preference', type=str, help='Author ordering preference')

    ## Mastodon Parameters
    agroup = aparser.add_argument_group('Mastodon Parameters')
    agroup.add_argument('--mastodon', action='store_true', help='Post announcements on Twitter')
    agroup.add_argument('--mastodon-api-key', type=str, help='Mastodon API Key')
    agroup.add_argument('--mastodon-url', type=str, help='Mastodon Server URL')

    ## Twitter Parameters
    agroup = aparser.add_argument_group('Twitter Parameters')
    agroup.add_argument('--twitter', action='store_true', help='Post announcements on Twitter')
    agroup.add_argument('--twitter-consumer-key', type=str, help='Twitter Consumer Key')
    agroup.add_argument('--twitter-consumer-secret', type=str, help='Twitter Consumer Secret')
    agroup.add_argument('--twitter-access-token', type=str, help='Twitter Access Token Key')
    agroup.add_argument('--twitter-access-secret', type=str, help='Twitter Access Token Secret')

    args = vars(aparser.parse_args())

    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    # If they want to post to a service, make sure they give us all the info
    # that we need to do this
    for check in ["mastodon", "twitter"]:
        if check not in args or not args[check]: continue
        LOG.debug("Checking %s input arguments", check)
        for k in args.keys():
            if k.startswith(check) and args[k] is None:
                LOG.error("Missing '%s' input parameter for %s", k, check)
                sys.exit(1)
        ## FOR
    ## IF

    ## ----------------------------------------------

    if not os.path.exists(args['dbpath']):
        raise Exception("Database file %s does not exist", args['dbpath'])
    db = sqlite3.connect(args['dbpath'])
    cur = db.cursor()

    ## Post new papers to Twitter
    sql = "SELECT * FROM papers WHERE twitter = 0 ORDER BY volume ASC, number ASC, "
    if 'preference' in args and args['preference']:
        sql += "CASE WHEN authors LIKE '%" + args['preference'] + "%' THEN NULL ELSE link END DESC"
    else:
        sql += "link"
    LOG.debug(sql)

    new_papers = [ ]
    for row in cur.execute(sql):
        paper = {
            "link":     row[0],
            "title":    row[1],
            "authors":  row[2],
            "volume":   row[3],
            "number":   row[4],
            "published":row[5],
        }
        new_papers.append(paper)
    ## FOR
    paper_count = 0
    for paper in new_papers:
        postMastodon(args, db, paper)
        paper_count += 1
        if args["twitter_limit"] and paper_count > args["twitter_limit"]:
            break
        LOG.warn("Sleeping for %d seconds..." % TWITTER_SLEEP_TIME)
        time.sleep(TWITTER_SLEEP_TIME)
    ## FOR
    
    db.close()
## MAIN
    
    

