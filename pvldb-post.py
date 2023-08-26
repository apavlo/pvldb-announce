#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import time
import argparse
import sqlite3
import requests
import tempfile
from pdf2image import convert_from_path
from pprint import pprint, pformat

import tweepy
from mastodon import Mastodon

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
## getImage
## ==============================================
def getImage(pdf_url):
    LOG.debug("Create thumbnail for '%s'", pdf_url)
    # Get the filename from the URL
    temp_dir = tempfile.gettempdir()
    pdf_filename = "pvldb-" + os.path.basename(pdf_url)
    pdf_path = os.path.join(temp_dir, pdf_filename)

    # Download the PDF file
    if not os.path.exists(pdf_path):
        response = requests.get(pdf_url)
        with open(pdf_path, 'wb') as pdf_file:
            pdf_file.write(response.content)
            LOG.debug("Downloaded PDF %s", pdf_path)

    # Convert the downloaded PDF to PNG images
    png_filename = f'{pdf_filename}.png'
    png_path = os.path.join(temp_dir, png_filename)
    if not os.path.exists(png_path):
        images = convert_from_path(pdf_path, first_page=0, last_page=1)
        if images:
            images[0].save(png_path, 'PNG')
            LOG.debug(f'Conversion successful. PNG image saved in temporary directory: {png_path}')
    return png_path

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
    if paper["authors"]:
        # Figure whether there is more than one author
        if paper["authors"].find(",") != -1:
            post += "\n👥 Authors"
        else:
            post += "\n👤 Author"
        post += ": %(authors)s" % paper

    if len(post)+24 > POST_MAX_NUM_CHARS["mastodon"]:
        remaining = POST_MAX_NUM_CHARS["mastodon"] - (len(post)+24)
        post = post[:remaining-3] + u"..."
    post += "\n📄 PDF: " + paper["link"]
    LOG.debug("%s [Length=%d]: %s", "mastodon", len(post), post)

    if not args["dry_run"]:
        if "image" in paper and paper["image"]:
            media = api.media_post(paper["image"], focus=(0, 0.85), description="Thumbnail: %(title)s" % paper)
            status = api.status_post(post, visibility='public', media_ids=media)
        else:
            status = api.status_post(post, visibility='public')
        LOG.info("Wrote post to %s [status=%s]", args["mastodon_url"], str(status))
    else:
        LOG.debug("Not posting to mastodon because dry-run is enabled")
    return

## ==============================================
## postTwitter
## ==============================================
def postTwitter(args, paper):
    LOG.info("Posting paper '%s' to twitter!" % paper["title"])

    auth = tweepy.OAuthHandler(args['twitter_api_key'], args['twitter_api_secret'])
    auth.set_access_token(args['twitter_access_token'], args['twitter_access_secret'])
    api = tweepy.API(auth)

    client = tweepy.Client(
        bearer_token=args['twitter_bearer_token'],
        consumer_key=args['twitter_api_key'],
        consumer_secret=args['twitter_api_secret'],
        access_token=args['twitter_api_key'],
        access_token_secret=args['twitter_access_secret']
    )

    post = "Vol:%(volume)d No:%(number)d → %(title)s" % paper
    if len(post) + 24 > POST_MAX_NUM_CHARS["twitter"]:
        remaining = POST_MAX_NUM_CHARS["twitter"] - (len(post) + 24)
        post = post[:remaining - 3] + "..."
    post += " " + paper["link"]
    LOG.debug("%s [Length=%d]" % (post, len(post)))

    if not args["dry_run"]:
        if not args['no_image'] and "image" in paper and paper["image"]:
            media = api.media_upload(paper["image"])
            LOG.debug(media)
            status = client.create_tweet(text=post, media_ids=[media.media_id])
            # status = api.update_status(status=post, media_ids=[media.media_id])
        else:
            status = client.create_tweet(text=post)
            # status = api.update_status(status=post)
    else:
        LOG.debug("Not posting to twitter because dry-run is enabled")

    LOG.info("Posted tweet [status=%s]", str(status))
    return

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='PVLDB Announcements Script')
    aparser.add_argument('dbpath', help='Database Path')
    aparser.add_argument("--debug", action='store_true')
    aparser.add_argument("--dry-run", action='store_true')

    aparser.add_argument('--limit', type=int, help='Number of papers to announce before stopping')
    aparser.add_argument('--no-image', action='store_true', help='Do not post images')
    aparser.add_argument('--sleep', type=int, default=POST_SLEEP_TIME, help='How many seconds to sleep between each post')
    aparser.add_argument('--preference', type=str, help='Author ordering preference')

    ## Mastodon Parameters
    agroup = aparser.add_argument_group('Mastodon Parameters')
    agroup.add_argument('--mastodon', action='store_true', help='Post announcements on Twitter')
    agroup.add_argument('--mastodon-api-key', type=str, help='Mastodon API Key')
    agroup.add_argument('--mastodon-url', type=str, help='Mastodon Server URL')

    ## Twitter Parameters
    agroup = aparser.add_argument_group('Twitter Parameters')
    agroup.add_argument('--twitter', action='store_true', help='Post announcements on Twitter')
    agroup.add_argument('--twitter-api-key', type=str, help='Twitter API Key')
    agroup.add_argument('--twitter-api-secret', type=str, help='Twitter API Secret')
    agroup.add_argument('--twitter-access-token', type=str, help='Twitter Access Token Key')
    agroup.add_argument('--twitter-access-secret', type=str, help='Twitter Access Token Secret')
    agroup.add_argument('--twitter-bearer-token', type=str, help='Twitter Bearer Token')

    args = vars(aparser.parse_args())

    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    # If they want to post to a service, make sure they give us all the info
    # that we need to do this
    post_targets = [ ]
    all_targets = ["mastodon", "twitter"]
    for target in all_targets:
        if target not in args or not args[target]: continue
        LOG.debug("Checking %s input arguments", target)
        for k in args.keys():
            if k.startswith(target) and args[k] is None:
                LOG.error("Missing '%s' input parameter for %s", k, target)
                sys.exit(1)
        ## FOR
        post_targets.append(target)
    if not post_targets:
        raise Exception("No post target was specified [%s]", ",".join(all_targets))

    ## ----------------------------------------------

    if not os.path.exists(args['dbpath']):
        raise Exception("Database file '%s' does not exist" % args['dbpath'])
    db = sqlite3.connect(args['dbpath'])
    cur = db.cursor()

    ## Post new papers

    where = [ ]
    for target in post_targets:
        if target in args and args[target]:
            where.append("%s = 0" % target)
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
        # Get a PNG image of the first page
        paper["image"] = getImage(paper["link"])
        assert paper["image"]

        for target in post_targets:
            try:
                if target == "mastodon":
                    postMastodon(args, paper)
                elif target == "twitter":
                    postTwitter(args, paper)
                cur = db.cursor()
                sql = "UPDATE papers SET %s = 1 WHERE link = ?" % target
                if not args["dry_run"]:
                    cur.execute(sql, (paper["link"],))
                    db.commit()
                else:
                    LOG.debug("Not updating %s [%s] because dry-run is enabled", os.path.basename(paper["link"]), target)
            except:
                raise
        paper_count += 1
        if args["limit"] and paper_count >= args["limit"]:
            break
        LOG.warning("Sleeping for %d seconds...", args["sleep"])
        time.sleep(args["sleep"])
    ## FOR
    
    db.close()
## MAIN
    
    

