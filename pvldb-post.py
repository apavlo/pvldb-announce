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
def postMastodon(args, paper):
    LOG.info("Posting paper '%s' to Mastodon!", paper["title"])

    api = Mastodon(
        access_token=args["mastodon_api_key"],
        api_base_url=args["mastodon_url"]
    )

    post = u"Vol:%(volume)d No:%(number)d → %(title)s" % paper
    if len(post)+24 > POST_MAX_NUM_CHARS["mastodon"]:
        remaining = POST_MAX_NUM_CHARS["mastodon"] - (len(post)+24)
        post = post[:remaining-3] + u"..."
    post += " " + paper["link"]
    
    LOG.debug("%s [Length=%d]", post, len(post))
    status = api.status_post(post)
    LOG.info("Wrote post to %s [status=%s]", args["mastodon_url"], str(status))

## ==============================================
## postTwitter
## ==============================================
def postTwitter(args, paper):
    LOG.info("Posting paper '%s' to twitter!" % paper["title"])

    api = None
    # api = twitter.Api(consumer_key=args["twitter_consumer_key"],
    #                   consumer_secret=args["twitter_consumer_secret"],
    #                   access_token_key=args["twitter_access_token"],
    #                   access_token_secret=args["twitter_access_secret"])

    # paper["separator"] = u"→".encode('unicode-escape')

    tweet = "Vol:%(volume)d No:%(number)d → %(title)s" % paper
    if len(tweet) + 24 > POST_MAX_NUM_CHARS["twitter"]:
        remaining = POST_MAX_NUM_CHARS["twitter"] - (len(tweet) + 24)
        tweet = tweet[:remaining - 3] + "..."
    tweet += " " + paper["link"]

    LOG.debug("%s [Length=%d]" % (tweet, len(tweet)))
    status = api.PostUpdate(tweet)
    LOG.info("Posted tweet [status=%s]", str(status))

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
    post_targets = [ ]
    for check in ["mastodon", "twitter"]:
        if check not in args or not args[check]: continue
        LOG.debug("Checking %s input arguments", check)
        for k in args.keys():
            if k.startswith(check) and args[k] is None:
                LOG.error("Missing '%s' input parameter for %s", k, check)
                sys.exit(1)
        ## FOR
        post_targets.append(check)
    ## IF

    ## ----------------------------------------------

    if not os.path.exists(args['dbpath']):
        raise Exception("Database file %s does not exist", args['dbpath'])
    db = sqlite3.connect(args['dbpath'])
    cur = db.cursor()

    ## Post new papers

    where = [ ]
    for target in post_targets:
        if check in args and args[check]:
            where.append("%s = 0" % check)
    assert len(where)

    sql = "SELECT * FROM papers WHERE %s " % " OR ".join(where)
    sql += "ORDER BY volume ASC, number ASC, "
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
        for target in post_targets:
            try:
                if target == "mastodon":
                    postMastodon(args, paper)
                elif target == "twitter":
                    # postTwitter(args, paper)
                    pass
                cur = db.cursor()
                sql = "UPDATE papers SET %s = 1 WHERE link = ?" % target
                cur.execute(sql, (paper["link"],))
                db.commit()
            except:
                raise
        paper_count += 1
        if args["limit"] and paper_count > args["limit"]:
            break
        LOG.warning("Sleeping for %d seconds..." % POST_SLEEP_TIME)
        time.sleep(POST_SLEEP_TIME)
    ## FOR
    
    db.close()
## MAIN
    
    

